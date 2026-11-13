import CryptoKit
import Foundation
import LocalAuthentication
import Observation
import Security
import SwiftUI

/// A PIN (and Face ID) in front of an app that is already signed in with Clerk.
/// It is a lock on this phone, not an account credential: the PIN never leaves
/// the device, only a salted SHA-256 of it is kept, in the Keychain, per account.
/// Five wrong tries signs out, and "Forgot PIN" is the same — Clerk's own
/// sign-in is the way back in.
@MainActor @Observable
final class AppLock {
    static let pinLength = 4
    static let maxAttempts = 5
    /// How long the app may sit in the background before it asks again.
    static let grace: TimeInterval = 5 * 60

    private(set) var isLocked = false
    private(set) var hasPin = false
    private(set) var biometricsEnabled = false
    private(set) var attemptsLeft = maxAttempts
    /// Set when the account has been offered a PIN and said not now.
    private(set) var declined = false

    private var account: Int?
    private var backgroundedAt: Date?

    // MARK: Account

    /// Called once `/api/me` has answered: load this account's lock, and lock
    /// at once if it has one (a cold start always asks).
    func attach(account id: Int) {
        guard account != id else { return }
        account = id
        let stored = Keychain.read(key("pin"))
        hasPin = stored != nil
        biometricsEnabled = hasPin && UserDefaults.standard.bool(forKey: key("faceid"))
        declined = UserDefaults.standard.bool(forKey: key("declined"))
        attemptsLeft = Self.maxAttempts
        isLocked = hasPin
    }

    func detach() {
        account = nil
        hasPin = false
        biometricsEnabled = false
        isLocked = false
        attemptsLeft = Self.maxAttempts
    }

    // MARK: Setting it

    func setPin(_ pin: String) {
        guard account != nil, pin.count == Self.pinLength else { return }
        let salt = (0..<16).map { _ in UInt8.random(in: 0...255) }
        let record = Data(salt) + Self.digest(pin, salt: salt)
        Keychain.write(record, key("pin"))
        hasPin = true
        attemptsLeft = Self.maxAttempts
        isLocked = false
    }

    func removePin() {
        Keychain.delete(key("pin"))
        UserDefaults.standard.removeObject(forKey: key("faceid"))
        hasPin = false
        biometricsEnabled = false
        isLocked = false
    }

    func setBiometrics(_ on: Bool) {
        biometricsEnabled = on && hasPin
        UserDefaults.standard.set(biometricsEnabled, forKey: key("faceid"))
    }

    func decline() {
        declined = true
        UserDefaults.standard.set(true, forKey: key("declined"))
    }

    // MARK: Unlocking

    enum Outcome { case unlocked, wrong, signOut }

    func check(_ pin: String) -> Outcome {
        guard let record = Keychain.read(key("pin")), record.count > 16 else { return .unlocked }
        let salt = Array(record.prefix(16))
        if Self.digest(pin, salt: salt) == record.dropFirst(16) {
            isLocked = false
            attemptsLeft = Self.maxAttempts
            return .unlocked
        }
        attemptsLeft -= 1
        return attemptsLeft <= 0 ? .signOut : .wrong
    }

    /// Face ID (or Touch ID); falls back to the keypad on any failure.
    func unlockWithBiometrics() async {
        guard biometricsEnabled, isLocked else { return }
        let ctx = LAContext()
        ctx.localizedFallbackTitle = "Enter PIN"
        guard ctx.canEvaluatePolicy(.deviceOwnerAuthenticationWithBiometrics, error: nil) else { return }
        if (try? await ctx.evaluatePolicy(.deviceOwnerAuthenticationWithBiometrics,
                                          localizedReason: "Unlock Platoon Manager")) == true {
            isLocked = false
            attemptsLeft = Self.maxAttempts
        }
    }

    static var biometryName: String? {
        let ctx = LAContext()
        guard ctx.canEvaluatePolicy(.deviceOwnerAuthenticationWithBiometrics, error: nil) else { return nil }
        switch ctx.biometryType {
        case .faceID: return "Face ID"
        case .touchID: return "Touch ID"
        case .opticID: return "Optic ID"
        default: return nil
        }
    }

    // MARK: Background

    func scenePhaseChanged(_ phase: ScenePhase) {
        switch phase {
        case .background:
            if backgroundedAt == nil { backgroundedAt = Date() }
        case .active:
            if let t = backgroundedAt, hasPin, Date().timeIntervalSince(t) >= Self.grace { isLocked = true }
            backgroundedAt = nil
        default: break
        }
    }

    // MARK: Plumbing

    private func key(_ name: String) -> String { "applock.\(name).\(account ?? 0)" }

    private static func digest(_ pin: String, salt: [UInt8]) -> Data {
        Data(SHA256.hash(data: Data(salt) + Data(pin.utf8)))
    }
}

/// The few Keychain calls the lock needs: this device only, after first unlock.
enum Keychain {
    private static func query(_ key: String) -> [String: Any] {
        [kSecClass as String: kSecClassGenericPassword,
         kSecAttrService as String: "com.platoonmanager.app.applock",
         kSecAttrAccount as String: key]
    }

    static func read(_ key: String) -> Data? {
        var q = query(key)
        q[kSecReturnData as String] = true
        q[kSecMatchLimit as String] = kSecMatchLimitOne
        var out: AnyObject?
        return SecItemCopyMatching(q as CFDictionary, &out) == errSecSuccess ? out as? Data : nil
    }

    static func write(_ data: Data, _ key: String) {
        delete(key)
        var q = query(key)
        q[kSecValueData as String] = data
        q[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        SecItemAdd(q as CFDictionary, nil)
    }

    static func delete(_ key: String) {
        SecItemDelete(query(key) as CFDictionary)
    }
}

// MARK: - Views

/// Dots for the digits typed so far.
struct PinDots: View {
    let count: Int
    var shake: Int = 0

    var body: some View {
        HStack(spacing: 18) {
            ForEach(0..<AppLock.pinLength, id: \.self) { i in
                Circle()
                    .strokeBorder(.primary, lineWidth: 1.5)
                    .background(Circle().fill(i < count ? Color.primary : .clear))
                    .frame(width: 15, height: 15)
            }
        }
        .modifier(Shake(animatableData: CGFloat(shake)))
        .accessibilityElement()
        .accessibilityLabel("\(count) of \(AppLock.pinLength) digits entered")
    }
}

/// A phone-style keypad: 1–9, then [left] 0 [delete].
struct PinPad: View {
    @Binding var digits: String
    var left: AnyView? = nil
    var onComplete: (String) -> Void

    var body: some View {
        VStack(spacing: 14) {
            ForEach([["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]], id: \.self) { row in
                HStack(spacing: 22) { ForEach(row, id: \.self) { key($0) } }
            }
            HStack(spacing: 22) {
                Group {
                    if let left { left } else { Color.clear }
                }
                .frame(width: 78, height: 78)
                key("0")
                Button {
                    if !digits.isEmpty { digits.removeLast() }
                } label: {
                    Image(systemName: "delete.left").font(.title2)
                        .frame(width: 78, height: 78)
                }
                .foregroundStyle(.primary)
                .opacity(digits.isEmpty ? 0 : 1)
                .accessibilityLabel("Delete")
            }
        }
    }

    private func key(_ d: String) -> some View {
        Button {
            guard digits.count < AppLock.pinLength else { return }
            Haptics.tap()
            digits.append(d)
            if digits.count == AppLock.pinLength {
                let pin = digits
                Task { @MainActor in
                    try? await Task.sleep(for: .milliseconds(120))   // let the last dot fill
                    onComplete(pin)
                }
            }
        } label: {
            Text(d).font(.system(size: 34, weight: .regular, design: .rounded))
                .frame(width: 78, height: 78)
                .background(Circle().fill(Color(.secondarySystemFill)))
        }
        .foregroundStyle(.primary)
        .buttonStyle(.plain)
    }
}

private struct Shake: GeometryEffect {
    var animatableData: CGFloat
    func effectValue(size: CGSize) -> ProjectionTransform {
        ProjectionTransform(CGAffineTransform(translationX: 10 * sin(animatableData * .pi * 4), y: 0))
    }
}

/// Covers everything while the app is locked.
struct LockScreen: View {
    @Environment(AppLock.self) private var lock
    @Environment(AppModel.self) private var app
    @State private var digits = ""
    @State private var shake = 0
    @State private var message: String?
    @State private var confirmForgot = false

    var body: some View {
        VStack(spacing: 28) {
            Spacer()
            Image(systemName: "lock.fill").font(.title).foregroundStyle(.secondary)
            Text("Enter your PIN").font(.title3.weight(.semibold))
            PinDots(count: digits.count, shake: shake)
            Text(message ?? " ").font(.footnote).foregroundStyle(.red).frame(height: 18)
            PinPad(digits: $digits, left: biometricKey, onComplete: submit)
            Button("Forgot PIN?") { confirmForgot = true }
                .font(.footnote)
                .padding(.top, 4)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
        .task { await lock.unlockWithBiometrics() }
        .confirmationDialog("Forgot your PIN?", isPresented: $confirmForgot, titleVisibility: .visible) {
            Button("Sign out", role: .destructive) { signOut() }
        } message: {
            Text("Sign out, then sign back in with your email to set a new PIN.")
        }
    }

    private var biometricKey: AnyView? {
        guard lock.biometricsEnabled, let name = AppLock.biometryName else { return nil }
        return AnyView(
            Button {
                Task { await lock.unlockWithBiometrics() }
            } label: {
                Image(systemName: name == "Touch ID" ? "touchid" : "faceid").font(.title)
            }
            .foregroundStyle(.tint)
            .accessibilityLabel("Unlock with \(name)")
        )
    }

    private func submit(_ pin: String) {
        switch lock.check(pin) {
        case .unlocked:
            digits = ""
            message = nil
        case .wrong:
            Haptics.error()
            withAnimation(.linear(duration: 0.35)) { shake += 1 }
            digits = ""
            message = lock.attemptsLeft == 1 ? "Wrong PIN. One more try before you are signed out."
                                             : "Wrong PIN. \(lock.attemptsLeft) tries left."
        case .signOut:
            signOut()
        }
    }

    private func signOut() {
        lock.removePin()
        Task { await app.signOut() }
    }
}

/// Choose a PIN: type it, then type it again.
struct SetPinView: View {
    @Environment(AppLock.self) private var lock
    @Environment(\.dismiss) private var dismiss
    var onFinish: () -> Void = {}
    @State private var first: String?
    @State private var digits = ""
    @State private var shake = 0
    @State private var mismatch = false
    @State private var offerBiometrics = false

    var body: some View {
        NavigationStack {
            VStack(spacing: 28) {
                Spacer()
                Text(first == nil ? "Choose a \(AppLock.pinLength)-digit PIN" : "Enter it again")
                    .font(.title3.weight(.semibold))
                Text(first == nil ? "You'll use it to open the app on this phone." : " ")
                    .font(.footnote).foregroundStyle(.secondary)
                PinDots(count: digits.count, shake: shake)
                Text(mismatch ? "Those didn't match. Start again." : " ")
                    .font(.footnote).foregroundStyle(.red).frame(height: 18)
                PinPad(digits: $digits, onComplete: entered)
                Spacer()
            }
            .frame(maxWidth: .infinity)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
            .alert("Use \(AppLock.biometryName ?? "Face ID") too?", isPresented: $offerBiometrics) {
                Button("Use \(AppLock.biometryName ?? "Face ID")") { finish(biometrics: true) }
                Button("PIN only", role: .cancel) { finish(biometrics: false) }
            } message: {
                Text("Unlock with \(AppLock.biometryName ?? "Face ID"), and the PIN when it doesn't work.")
            }
        }
        .interactiveDismissDisabled()
    }

    private func entered(_ pin: String) {
        guard let first else {
            self.first = pin
            digits = ""
            mismatch = false
            return
        }
        guard pin == first else {
            Haptics.error()
            withAnimation(.linear(duration: 0.35)) { shake += 1 }
            self.first = nil
            digits = ""
            mismatch = true
            return
        }
        lock.setPin(pin)
        Haptics.success()
        if AppLock.biometryName != nil { offerBiometrics = true } else { finish(biometrics: false) }
    }

    private func finish(biometrics: Bool) {
        lock.setBiometrics(biometrics)
        onFinish()
        dismiss()
    }
}

/// More → App lock.
struct AppLockSettingsView: View {
    @Environment(AppLock.self) private var lock
    @State private var setting = false
    @State private var confirmRemove = false

    var body: some View {
        Form {
            Section {
                if lock.hasPin {
                    Button("Change PIN") { setting = true }
                    if let name = AppLock.biometryName {
                        Toggle("Unlock with \(name)", isOn: Binding(
                            get: { lock.biometricsEnabled },
                            set: { lock.setBiometrics($0) }))
                    }
                    Button("Turn off PIN", role: .destructive) { confirmRemove = true }
                } else {
                    Button("Set a PIN") { setting = true }
                }
            } footer: {
                Text("The app asks for it when it opens and after \(Int(AppLock.grace / 60)) minutes in the background. It stays on this phone. \(AppLock.maxAttempts) wrong tries signs you out.")
            }
        }
        .navigationTitle("App lock")
        .sheet(isPresented: $setting) { SetPinView() }
        .confirmationDialog("Turn off the PIN?", isPresented: $confirmRemove, titleVisibility: .visible) {
            Button("Turn off", role: .destructive) { lock.removePin() }
        }
    }
}

/// Shown once after sign-in on a phone with no PIN yet.
struct PinOfferView: View {
    @Environment(AppLock.self) private var lock
    @Environment(\.dismiss) private var dismiss
    @State private var setting = false

    var body: some View {
        VStack(spacing: 20) {
            Spacer()
            Image(systemName: "lock.shield").font(.system(size: 56)).foregroundStyle(.tint)
            Text("Lock the app with a PIN").font(.title2.weight(.semibold))
            Text("Open the app with a \(AppLock.pinLength)-digit PIN\(AppLock.biometryName.map { " or \($0)" } ?? "") instead of signing in again. You stay signed in either way.")
                .multilineTextAlignment(.center).foregroundStyle(.secondary)
                .padding(.horizontal)
            Spacer()
            Button {
                setting = true
            } label: {
                Text("Set a PIN").frame(maxWidth: .infinity).padding(.vertical, 6)
            }
            .buttonStyle(.borderedProminent)
            Button("Not now") {
                lock.decline()
                dismiss()
            }
            .padding(.bottom)
        }
        .padding()
        .presentationDetents([.medium])
        .sheet(isPresented: $setting) {
            SetPinView(onFinish: { dismiss() })
        }
    }
}
