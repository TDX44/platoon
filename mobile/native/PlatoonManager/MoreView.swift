import SwiftUI

/// Report history, email alerts, the things that stay on the website, and the account.
struct MoreView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var web: URL?
    @State private var confirmSignOut = false

    var body: some View {
        NavigationStack {
            List {
                if let me = app.me {
                    Section {
                        VStack(alignment: .leading, spacing: 2) {
                            Text(me.displayName).font(.headline)
                            if let email = me.email, !email.isEmpty {
                                Text(email).font(.subheadline).foregroundStyle(.secondary)
                            }
                            Text("\(me.isOwner ? "Owner" : "Leader") · \(me.unitName ?? "")")
                                .font(.caption).foregroundStyle(.secondary)
                        }
                    }
                }

                Section("This unit — \(app.selectedUnit?.name ?? "")") {
                    NavigationLink {
                        ReportHistoryView()
                    } label: {
                        Label("Report history", systemImage: "clock.arrow.circlepath")
                    }
                    if let slug = app.selectedUnit?.slug {
                        webRow("TDY lists (schools, locations)", "list.bullet", "\(slug)/schools")
                        webRow("Sub-units", "point.3.connected.trianglepath.dotted", "\(slug)/units")
                        webRow("Audit log", "doc.text.magnifyingglass", "\(slug)/audit")
                        webRow("Logo", "photo", "\(slug)/settings")
                        webRow("Import alpha roster", "square.and.arrow.down", "\(slug)/accountability")
                    }
                }

                Section {
                    Picker(selection: primaryBinding) {
                        Text(app.me?.unitName.map { "\($0) (default)" } ?? "Default").tag(Int?.none)
                        ForEach(app.tree.subtree(of: app.me?.unitId ?? 0), id: \.self) { id in
                            if id != app.me?.unitId, let u = app.tree.unit(id) {
                                Text(u.name).tag(Int?.some(id))
                            }
                        }
                    } label: {
                        Label("Primary unit", systemImage: "star")
                    }
                } footer: {
                    Text("The unit the app opens on, on this phone. You can still switch to any unit you lead.")
                }

                Section("Your account") {
                    NavigationLink {
                        NotificationPrefsView()
                    } label: {
                        Label("Email alerts", systemImage: "bell")
                    }
                    if let slug = app.selectedUnit?.slug {
                        webRow("Profile", "person.crop.circle", "\(slug)/settings/profile")
                    }
                }

                if let slug = app.selectedUnit?.slug {
                    Section("Organization") {
                        webRow("Accounts & invites", "person.2.badge.key", "\(slug)/settings/people")
                        webRow("Time zone", "globe", "\(slug)/settings/timezone")
                        webRow("Backup & restore", "externaldrive", "\(slug)/settings/data")
                    }
                }

                Section {
                    Button("Sign out", role: .destructive) { confirmSignOut = true }
                } footer: {
                    Text("Platoon Manager \(Bundle.main.appVersion). Settings marked ↗ open on the website.")
                }
            }
            .navigationTitle("More")
            .sheet(item: $web) { SafariView(url: $0).ignoresSafeArea() }
            .confirmationDialog("Sign out?", isPresented: $confirmSignOut, titleVisibility: .visible) {
                Button("Sign out", role: .destructive) { Task { await app.signOut() } }
            }
        }
    }

    private var primaryBinding: Binding<Int?> {
        Binding(get: { app.primaryUnitId }, set: { new in
            app.primaryUnitId = new
            app.selectedUnitId = app.homeUnitId
        })
    }

    private func webRow(_ title: String, _ icon: String, _ path: String) -> some View {
        Button {
            web = WebPage.url(path)
        } label: {
            HStack {
                Label(title, systemImage: icon).foregroundStyle(.primary)
                Spacer()
                Image(systemName: "arrow.up.right").font(.caption).foregroundStyle(.secondary)
            }
        }
    }
}

extension Bundle {
    var appVersion: String {
        let v = infoDictionary?["CFBundleShortVersionString"] as? String ?? "?"
        let b = infoDictionary?["CFBundleVersion"] as? String ?? "?"
        return "\(v) (\(b))"
    }
}

/// Per-account email alerts (`GET`/`PUT /api/me/notifications`).
struct NotificationPrefsView: View {
    @Environment(AppModel.self) private var app
    @State private var prefs: NotificationPrefs?
    @State private var error: String?
    @State private var saved = false

    var body: some View {
        Form {
            if let p = prefs {
                if p.emailEnabled == false {
                    Section {
                        Text("Email alerts are not switched on for this server yet.").foregroundStyle(.secondary)
                    }
                }
                Section {
                    Toggle("Accountability not complete", isOn: binding(\.accountabilityEnabled))
                    if p.accountabilityEnabled {
                        TimeField(title: "Check at", hhmm: binding(\.accountabilityTime))
                    }
                } footer: {
                    Text("Emails you if anyone in your unit is still unaccounted for at this time.")
                }
                Section {
                    Toggle("Morning digest", isOn: binding(\.digestEnabled))
                    if p.digestEnabled {
                        TimeField(title: "Send at", hhmm: binding(\.digestTime))
                    }
                } footer: {
                    Text("Who is due back today, who starts an absence today, and anyone overdue.")
                }
                if let email = p.email, !email.isEmpty {
                    Section { LabeledContent("Sent to", value: email) }
                }
            } else if error == nil {
                ProgressView()
            }
            if let error { Text(error).foregroundStyle(.red) }
        }
        .navigationTitle("Email alerts")
        .task {
            do { prefs = try await app.api.get("me/notifications") } catch { self.error = error.localizedDescription }
        }
    }

    private func binding<T>(_ key: WritableKeyPath<NotificationPrefs, T>) -> Binding<T> {
        Binding(get: { prefs![keyPath: key] }, set: { new in
            prefs![keyPath: key] = new
            Task { await save() }
        })
    }

    private func save() async {
        guard let p = prefs else { return }
        do {
            prefs = try await app.api.put("me/notifications", [
                "accountability_enabled": p.accountabilityEnabled, "accountability_time": p.accountabilityTime,
                "digest_enabled": p.digestEnabled, "digest_time": p.digestTime,
            ])
            error = nil
        } catch { self.error = error.localizedDescription }
    }
}

/// An "HH:MM" bound to a wheel time picker (the tenant's clock; no zone math).
struct TimeField: View {
    let title: String
    @Binding var hhmm: String

    var body: some View {
        let utc = TimeZone(identifier: "UTC")!
        let binding = Binding<Date>(
            get: {
                let parts = hhmm.split(separator: ":").compactMap { Int($0) }
                let minutes = parts.count == 2 ? parts[0] * 60 + parts[1] : 6 * 60 + 30
                return Date(timeIntervalSince1970: Double(minutes * 60))
            },
            set: {
                var cal = Calendar(identifier: .gregorian)
                cal.timeZone = utc
                let c = cal.dateComponents([.hour, .minute], from: $0)
                hhmm = String(format: "%02d:%02d", c.hour ?? 0, c.minute ?? 0)
            })
        DatePicker(title, selection: binding, displayedComponents: .hourAndMinute)
            .environment(\.timeZone, utc)
    }
}
