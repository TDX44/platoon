import SafariServices
import SwiftUI

// MARK: Status look

enum StatusStyle {
    static func color(_ status: String) -> Color {
        switch status {
        case "present": return .green
        case "leave", "pass": return .blue
        case "tdy": return .purple
        case "late": return .orange
        case "excused": return .teal
        case "other": return .gray
        case "ftr": return .red
        case "unaccounted": return .orange
        default: return .secondary
        }
    }

    static func symbol(_ status: String) -> String {
        switch status {
        case "present": return "checkmark.circle.fill"
        case "leave": return "beach.umbrella"
        case "pass": return "ticket"
        case "tdy": return "airplane"
        case "late": return "clock"
        case "excused": return "cross.case"
        case "other": return "ellipsis.circle"
        case "ftr": return "exclamationmark.triangle.fill"
        default: return "circle"
        }
    }
}

struct StatusBadge: View {
    let status: String
    var label: String? = nil

    var body: some View {
        Text(label ?? statusLabel(status))
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 3)
            .foregroundStyle(StatusStyle.color(status))
            .background(StatusStyle.color(status).opacity(0.15), in: Capsule())
    }
}

// MARK: Website (things the app never does natively)

enum WebPage {
    static func url(_ path: String) -> URL {
        Config.apiBase.appending(path: path)
    }
}

struct SafariView: UIViewControllerRepresentable {
    let url: URL
    func makeUIViewController(context: Context) -> SFSafariViewController {
        SFSafariViewController(url: url)
    }
    func updateUIViewController(_ controller: SFSafariViewController, context: Context) {}
}

extension URL: @retroactive Identifiable {
    public var id: String { absoluteString }
}

// MARK: Toast

struct ToastModifier: ViewModifier {
    @Binding var message: String?

    func body(content: Content) -> some View {
        content.overlay(alignment: .bottom) {
            if let message {
                Text(message)
                    .font(.subheadline)
                    .padding(.horizontal, 16)
                    .padding(.vertical, 10)
                    .background(.regularMaterial, in: Capsule())
                    .padding(.bottom, 24)
                    .padding(.horizontal, 16)
                    .transition(.move(edge: .bottom).combined(with: .opacity))
                    .task(id: message) {
                        try? await Task.sleep(for: .seconds(3))
                        withAnimation { self.message = nil }
                    }
                    .onTapGesture { withAnimation { self.message = nil } }
            }
        }
        .animation(.default, value: message)
    }
}

extension View {
    func toast(_ message: Binding<String?>) -> some View { modifier(ToastModifier(message: message)) }
}

// MARK: Haptics

enum Haptics {
    @MainActor static func success() { UINotificationFeedbackGenerator().notificationOccurred(.success) }
    @MainActor static func warning() { UINotificationFeedbackGenerator().notificationOccurred(.warning) }
    @MainActor static func tap() { UIImpactFeedbackGenerator(style: .light).impactOccurred() }
}

// MARK: Phone

enum Phone {
    /// A tel:/sms: URL for a stored phone value (`phoneE164`, the web's rule),
    /// or nil when it cannot be dialled — a DSN, an extension, an unknown country.
    static func url(_ scheme: String, _ value: String?) -> URL? {
        guard let value else { return nil }
        let e164 = phoneE164(value)
        return e164.isEmpty ? nil : URL(string: "\(scheme):\(e164)")
    }
}
