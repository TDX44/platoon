import SwiftUI

/// "Quick add absence…": type a sentence, the server asks Claude, and each
/// proposal opens pre-filled in the ordinary status sheet. Nothing is booked
/// until the leader presses Save there.
struct QuickAddView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @Environment(\.dismiss) private var dismiss
    @State private var text = ""
    @State private var busy = false
    @State private var error: String?
    @State private var queue: [QuickProposal] = []
    @State private var current: StatusTarget?
    @State private var skipped: [String] = []

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("e.g. Boundy is going to Fort Bliss for 2 weeks starting Jan 1 for live aircraft",
                              text: $text, axis: .vertical)
                        .lineLimit(3...8)
                        .submitLabel(.send)
                        // A multi-line field puts Return in the text rather than
                        // submitting; take it back out and send instead.
                        .onChange(of: text) { _, new in
                            guard new.contains("\n") else { return }
                            text = new.replacingOccurrences(of: "\n", with: " ")
                                .trimmingCharacters(in: .whitespaces)
                            send()
                        }
                } footer: {
                    Text("Name the soldier, what, and when. Each absence opens for you to check before it is saved.")
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
                if !skipped.isEmpty {
                    Section("Couldn't match") {
                        ForEach(skipped, id: \.self) { Text($0).font(.callout) }
                    }
                }
            }
            .navigationTitle("Quick add")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    if busy { ProgressView() } else {
                        Button("Read it") { send() }
                            .disabled(text.trimmingCharacters(in: .whitespaces).isEmpty)
                    }
                }
            }
            .sheet(item: $current) { StatusSheet(target: $0) }
        }
    }

    private func send() {
        guard !busy, !text.trimmingCharacters(in: .whitespaces).isEmpty else { return }
        Task { await ask() }
    }

    private func ask() async {
        busy = true
        defer { busy = false }
        error = nil
        skipped = []
        do {
            let r: QuickProposals = try await app.api.post("absences/quick", ["text": text, "unit_id": store.unitId])
            if r.proposals.isEmpty {
                error = "Could not find an absence in that. Try naming the soldier and the dates."
                return
            }
            queue = r.proposals
            next()
        } catch {
            self.error = error.localizedDescription
        }
    }

    /// `nextQuickProposal()`: one at a time; a name not on the roster is reported, not guessed.
    private func next() {
        while let pr = queue.first {
            queue.removeFirst()
            if let id = pr.personId, store.soldier(id) != nil {
                current = StatusTarget(soldierId: id, proposal: pr, onDone: {
                    if queue.isEmpty {
                        if skipped.isEmpty { dismiss() }
                    } else {
                        Task { @MainActor in
                            try? await Task.sleep(for: .milliseconds(450))
                            next()
                        }
                    }
                })
                return
            }
            skipped.append("\"\(pr.name ?? "?")\"\(pr.question.map { ": \($0)" } ?? "")")
        }
    }
}
