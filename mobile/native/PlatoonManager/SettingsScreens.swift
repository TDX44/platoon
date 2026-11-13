import ClerkKit
import ClerkKitUI
import PhotosUI
import SwiftUI
import UniformTypeIdentifiers

/// The settings the website keeps under Settings, native: TDY lists,
/// sub-units, audit log, logo, alpha-roster import, profile, accounts and
/// invites, time zone, backup and restore. Every rule stays on the server;
/// these screens only call the same routes the web page does.

let unitKinds = ["company", "platoon", "squad", "team", "section", "detachment", "flight", "crew"]

private func errorText(_ error: Error) -> String { error.localizedDescription }

// MARK: TDY lists

/// The unit's schools and locations. Order is significant: it is the order the
/// absence form's pickers show, so the screen reorders by hand and never sorts.
struct TDYListsView: View {
    @Environment(RosterStore.self) private var store
    @State private var newSchool = ""
    @State private var newLocation = ""
    @State private var error: String?

    var body: some View {
        List {
            listSection("Schools / courses", kind: "schools", items: store.schools, draft: $newSchool)
            listSection("Locations", kind: "locations", items: store.locations, draft: $newLocation)
            if let error { Text(error).foregroundStyle(.red) }
        }
        .navigationTitle("TDY lists")
        .toolbar { EditButton() }
    }

    private func listSection(_ title: String, kind: String, items: [String], draft: Binding<String>) -> some View {
        Section {
            ForEach(items, id: \.self) { Text($0) }
                .onDelete { idx in
                    var list = items
                    list.remove(atOffsets: idx)
                    save(kind, list)
                }
                .onMove { from, to in
                    var list = items
                    list.move(fromOffsets: from, toOffset: to)
                    save(kind, list)
                }
            HStack {
                TextField("Add…", text: draft)
                    .submitLabel(.done)
                    .onSubmit { add(kind, items, draft) }
                Button("Add") { add(kind, items, draft) }
                    .disabled(draft.wrappedValue.trimmingCharacters(in: .whitespaces).isEmpty)
            }
        } header: {
            Text(title)
        } footer: {
            Text("The absence form lists these in this order. Tap Edit to reorder or delete.")
        }
    }

    private func add(_ kind: String, _ items: [String], _ draft: Binding<String>) {
        let v = draft.wrappedValue.trimmingCharacters(in: .whitespaces)
        guard !v.isEmpty else { return }
        if items.contains(where: { $0.caseInsensitiveCompare(v) == .orderedSame }) {
            error = "\"\(v)\" is already on the list."
            return
        }
        draft.wrappedValue = ""
        save(kind, items + [v])
    }

    private func save(_ kind: String, _ list: [String]) {
        Task {
            do {
                try await store.saveList(kind: kind, list)
                error = nil
            } catch { self.error = errorText(error) }
        }
    }
}

// MARK: Sub-units

struct SubUnitsView: View {
    @Environment(AppModel.self) private var app
    @State private var editing: UnitEdit?
    @State private var confirmDelete: UnitNode?
    @State private var error: String?

    struct UnitEdit: Identifiable {
        var unit: UnitNode?
        var parentId: Int
        var id: String { unit.map { "u\($0.id)" } ?? "new\(parentId)" }
    }

    var body: some View {
        let tree = app.tree
        let parent = app.selectedUnit
        List {
            if let parent {
                Section {
                    row(parent, depth: 0, tree: tree)
                } header: {
                    Text("This unit")
                }
                Section {
                    let ids = tree.subtree(of: parent.id).filter { $0 != parent.id }
                    if ids.isEmpty {
                        Text("No sub-units yet.").foregroundStyle(.secondary)
                    }
                    ForEach(ids, id: \.self) { id in
                        if let u = tree.unit(id) { row(u, depth: depth(of: id, under: parent.id, tree: tree), tree: tree) }
                    }
                    Button("Add sub-unit", systemImage: "plus") { editing = UnitEdit(unit: nil, parentId: parent.id) }
                } header: {
                    Text("Sub-units")
                } footer: {
                    Text("Swipe a unit to rename, add under it, or delete it. A unit can be deleted only once it has no sub-units, soldiers or accounts.")
                }
            }
            if let error { Section { Text(error).foregroundStyle(.red) } }
        }
        .navigationTitle("Sub-units")
        .refreshable { await app.refreshUnits() }
        .sheet(item: $editing) { UnitEditSheet(edit: $0) }
        .confirmationDialog("Delete \(confirmDelete?.name ?? "")?", isPresented: Binding(
            get: { confirmDelete != nil }, set: { if !$0 { confirmDelete = nil } }), titleVisibility: .visible) {
            Button("Delete", role: .destructive) {
                if let u = confirmDelete { Task { await delete(u) } }
            }
        }
    }

    private func depth(of id: Int, under top: Int, tree: UnitTree) -> Int {
        var d = 0
        var cur = tree.unit(id)?.parentId
        while let c = cur, c != top { d += 1; cur = tree.unit(c)?.parentId }
        return d
    }

    private func row(_ u: UnitNode, depth: Int, tree: UnitTree) -> some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text(u.name)
                Text("\(u.kind.capitalized) · \(tree.headcount(of: u.id)) assigned")
                    .font(.caption).foregroundStyle(.secondary)
            }
            .padding(.leading, CGFloat(depth) * 16)
        }
        .swipeActions {
            if u.parentId != nil {
                Button("Delete", role: .destructive) { confirmDelete = u }
            }
            Button("Rename") { editing = UnitEdit(unit: u, parentId: u.parentId ?? u.id) }.tint(.blue)
            Button("Add under") { editing = UnitEdit(unit: nil, parentId: u.id) }.tint(.green)
        }
        .contextMenu {
            Button("Rename", systemImage: "pencil") { editing = UnitEdit(unit: u, parentId: u.parentId ?? u.id) }
            Button("Add sub-unit under it", systemImage: "plus") { editing = UnitEdit(unit: nil, parentId: u.id) }
            if u.parentId != nil {
                Button("Delete", systemImage: "trash", role: .destructive) { confirmDelete = u }
            }
        }
    }

    private func delete(_ u: UnitNode) async {
        do {
            try await app.api.delete("units/\(u.id)")
            Haptics.success()
            error = nil
            await app.refreshUnits()
        } catch { self.error = errorText(error) }
    }
}

private struct UnitEditSheet: View {
    @Environment(AppModel.self) private var app
    @Environment(\.dismiss) private var dismiss
    let edit: SubUnitsView.UnitEdit
    @State private var name = ""
    @State private var kind = "platoon"
    @State private var error: String?
    @State private var saving = false

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("Name", text: $name)
                    Picker("Kind", selection: $kind) {
                        ForEach(unitKinds, id: \.self) { Text($0.capitalized).tag($0) }
                    }
                } footer: {
                    if edit.unit == nil, let p = app.tree.unit(edit.parentId) {
                        Text("Added under \(p.name).")
                    }
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
            }
            .navigationTitle(edit.unit == nil ? "New sub-unit" : "Rename unit")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save() } }
                        .disabled(saving || name.trimmingCharacters(in: .whitespaces).isEmpty)
                }
            }
            .onAppear {
                name = edit.unit?.name ?? ""
                kind = edit.unit?.kind ?? childKind
            }
        }
    }

    /// A sensible default one level down: a company's children are platoons, and so on.
    private var childKind: String {
        switch app.tree.unit(edit.parentId)?.kind {
        case "company": return "platoon"
        case "platoon": return "squad"
        case "squad": return "team"
        default: return "section"
        }
    }

    private func save() async {
        saving = true
        defer { saving = false }
        do {
            if let u = edit.unit {
                let _: UnitNode = try await app.api.put("units/\(u.id)", ["name": name, "kind": kind])
            } else {
                let _: UnitNode = try await app.api.post("units", ["name": name, "kind": kind, "parent_id": edit.parentId])
            }
            Haptics.success()
            await app.refreshUnits()
            dismiss()
        } catch { self.error = errorText(error) }
    }
}

// MARK: Audit log

struct AuditLogView: View {
    @Environment(AppModel.self) private var app
    @State private var rows: [AuditRow] = []
    @State private var query = ""
    @State private var error: String?
    @State private var loaded = false

    var body: some View {
        List {
            if let error { Text(error).foregroundStyle(.red) }
            if loaded && filtered.isEmpty {
                ContentUnavailableView.search(text: query)
            }
            ForEach(filtered) { r in
                VStack(alignment: .leading, spacing: 3) {
                    HStack {
                        Text(r.action ?? "").font(.subheadline.weight(.semibold))
                        Spacer()
                        Text(r.timestamp ?? "").font(.caption.monospacedDigit()).foregroundStyle(.secondary)
                    }
                    if let d = r.details, !d.isEmpty { Text(d).font(.callout) }
                    HStack(spacing: 6) {
                        if let u = r.username, !u.isEmpty { Text(u) }
                        if let id = r.unitId, let unit = app.tree.unit(id) { Text("· \(unit.name)") }
                    }
                    .font(.caption).foregroundStyle(.secondary)
                }
                .textSelection(.enabled)
            }
        }
        .navigationTitle("Audit log")
        .searchable(text: $query, prompt: "Action, details or user")
        .task { await load() }
        .refreshable { await load() }
    }

    private var filtered: [AuditRow] {
        let q = query.trimmingCharacters(in: .whitespaces).lowercased()
        guard !q.isEmpty else { return rows }
        return rows.filter {
            [$0.action, $0.details, $0.username].compactMap { $0?.lowercased() }.contains { $0.contains(q) }
        }
    }

    private func load() async {
        guard let unit = app.selectedUnitId else { return }
        do {
            rows = try await app.api.get("audit", query: ["unit": String(unit), "limit": "500"])
            error = nil
        } catch { self.error = errorText(error) }
        loaded = true
    }
}

// MARK: Logo

/// A unit's logo: its own, or the nearest ancestor's. The server re-checks the
/// PNG (512 x 512 at most, 400 KB decoded), so this only gets it under that.
struct LogoView: View {
    @Environment(AppModel.self) private var app
    @State private var image: UIImage?
    @State private var pick: PhotosPickerItem?
    @State private var busy = false
    @State private var error: String?
    @State private var loaded = false

    var body: some View {
        let unit = app.selectedUnit
        let ref = unit?.logo
        let own = ref?.unitId != nil && ref?.unitId == unit?.id
        Form {
            Section {
                HStack {
                    Spacer()
                    Group {
                        if let image {
                            Image(uiImage: image).resizable().scaledToFit()
                        } else if loaded {
                            Image(systemName: "shield.lefthalf.filled").font(.system(size: 72)).foregroundStyle(.secondary)
                        } else {
                            ProgressView()
                        }
                    }
                    .frame(width: 160, height: 160)
                    .clipShape(RoundedRectangle(cornerRadius: 12))
                    Spacer()
                }
            } footer: {
                if own {
                    Text("\(unit?.name ?? "This unit")'s own logo. Units below it show it too unless they set their own.")
                } else if let name = ref?.name, ref?.unitId != nil {
                    Text("Inherited from \(name). Choose a picture to give \(unit?.name ?? "this unit") its own.")
                } else {
                    Text("No logo yet: the app's own mark is shown.")
                }
            }
            Section {
                PhotosPicker(selection: $pick, matching: .images) {
                    Label(own ? "Replace logo" : "Choose a logo", systemImage: "photo")
                }
                .disabled(busy)
                if own {
                    Button("Remove logo", systemImage: "trash", role: .destructive) { Task { await remove() } }
                        .disabled(busy)
                }
            } footer: {
                Text("A square picture works best. It is shrunk to 512 x 512.")
            }
            if let error { Section { Text(error).foregroundStyle(.red) } }
        }
        .navigationTitle("Logo")
        .task(id: "\(unit?.id ?? 0)-\(ref?.v ?? "")") { await load() }
        .onChange(of: pick) { _, item in
            guard let item else { return }
            Task { await upload(item) }
        }
    }

    private func load() async {
        guard let id = app.selectedUnitId else { return }
        if let data = try? await app.api.data("units/\(id)/logo", query: ["v": app.selectedUnit?.logo?.v ?? ""]) {
            image = UIImage(data: data)
        } else {
            image = nil
        }
        loaded = true
    }

    private func upload(_ item: PhotosPickerItem) async {
        guard let id = app.selectedUnitId else { return }
        busy = true
        defer { busy = false; pick = nil }
        do {
            guard let data = try await item.loadTransferable(type: Data.self), let picked = UIImage(data: data) else {
                error = "That picture could not be read."
                return
            }
            guard let png = Self.logoPNG(picked) else {
                error = "That picture is too detailed to fit in 400 KB. Try a simpler one."
                return
            }
            try await app.api.send("PUT", "units/\(id)/logo", ["png_base64": png.base64EncodedString()])
            Haptics.success()
            error = nil
            image = UIImage(data: png)
            await app.refreshUnits()
        } catch { self.error = errorText(error) }
    }

    private func remove() async {
        guard let id = app.selectedUnitId else { return }
        busy = true
        defer { busy = false }
        do {
            try await app.api.delete("units/\(id)/logo")
            await app.refreshUnits()
            await load()
        } catch { self.error = errorText(error) }
    }

    /// Fit within 512 x 512 and under 400 KB, shrinking further if a detailed
    /// photo still comes out too big.
    static func logoPNG(_ image: UIImage) -> Data? {
        for side in [512.0, 384, 256, 192] {
            let scale = min(1, side / max(image.size.width, image.size.height))
            let size = CGSize(width: (image.size.width * scale).rounded(), height: (image.size.height * scale).rounded())
            let format = UIGraphicsImageRendererFormat()
            format.scale = 1
            let png = UIGraphicsImageRenderer(size: size, format: format).pngData { _ in
                image.draw(in: CGRect(origin: .zero, size: size))
            }
            if png.count <= 400 * 1024 { return png }
        }
        return nil
    }
}

// MARK: Import alpha roster

/// The Import modal: a pasted list ("RANK Last, First" a line) or a CSV/TSV
/// with a header row. Everything a row MEANS is POST /api/personnel/import's
/// call, asked with dry_run first so the preview cannot promise what the
/// import then refuses.
struct ImportRosterView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var text = ""
    @State private var mapping: [String] = []
    @State private var includeDuplicates = false
    @State private var preview: ImportPreview?
    @State private var busy = false
    @State private var error: String?
    @State private var picking = false
    @State private var done: String?

    var body: some View {
        let table = ImportParse.parseDelimited(text)
        let spreadsheet = ImportParse.isSpreadsheet(table)
        Form {
            Section {
                TextField("CPT Smith, John\nSGT Jones, Mary", text: $text, axis: .vertical)
                    .lineLimit(5...12)
                    .font(.system(.callout, design: .monospaced))
                    .autocorrectionDisabled()
                    .textInputAutocapitalization(.never)
                Button("Choose a CSV or spreadsheet file", systemImage: "doc") { picking = true }
            } header: {
                Text("Paste or choose a file")
            } footer: {
                Text("One soldier a line (RANK Last, First), or a spreadsheet saved as CSV with a header row naming Last and First (or Name).")
            }
            if spreadsheet {
                Section("Columns") {
                    ForEach(Array(table[0].enumerated()), id: \.offset) { i, header in
                        Picker(header.isEmpty ? "Column \(i + 1)" : header, selection: Binding(
                            get: { i < mapping.count ? mapping[i] : "" },
                            set: { v in
                                syncMapping(table)
                                mapping[i] = v
                                preview = nil
                            })) {
                            ForEach(ImportParse.targets, id: \.0) { Text($0.1).tag($0.0) }
                        }
                    }
                }
            }
            Section {
                Toggle("Import likely duplicates too", isOn: $includeDuplicates)
                    .onChange(of: includeDuplicates) { preview = nil }
                Button("Preview") { Task { await run(dryRun: true) } }
                    .disabled(busy || text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            } footer: {
                Text("Soldiers go into \(app.selectedUnit?.name ?? "this unit") unless their row names one of its sub-units.")
            }
            if let p = preview {
                Section {
                    LabeledContent("To add", value: "\(p.counts.add)")
                    if p.counts.duplicate > 0 { LabeledContent("Likely duplicates (skipped)", value: "\(p.counts.duplicate)") }
                    if p.counts.error > 0 { LabeledContent("Errors (left out)", value: "\(p.counts.error)") }
                    Button("Import \(p.counts.add) soldier\(p.counts.add == 1 ? "" : "s")") {
                        Task { await run(dryRun: false) }
                    }
                    .disabled(busy || p.counts.add == 0)
                }
                Section("Rows") {
                    ForEach(p.rows, id: \.index) { r in
                        VStack(alignment: .leading, spacing: 2) {
                            HStack {
                                Text("\(r.rank ?? "") \(r.last ?? ""), \(r.first ?? "")")
                                Spacer()
                                Text(r.action == "add" ? "Add" : r.action == "duplicate" ? "Duplicate" : "Error")
                                    .font(.caption)
                                    .foregroundStyle(r.action == "add" ? Color.green : r.action == "duplicate" ? .orange : .red)
                            }
                            if let e = r.error { Text(e).font(.caption).foregroundStyle(.red) }
                            if let u = r.unitName, !u.isEmpty {
                                Text(r.unitMatched == false ? "\(u) (\"\(r.unitLabel ?? "")\" not found)" : u)
                                    .font(.caption).foregroundStyle(r.unitMatched == false ? .orange : .secondary)
                            }
                        }
                    }
                }
            }
            if let done { Section { Label(done, systemImage: "checkmark.circle.fill").foregroundStyle(.green) } }
            if let error { Section { Text(error).foregroundStyle(.red) } }
        }
        .navigationTitle("Import roster")
        .onChange(of: text) {
            preview = nil
            done = nil
            mapping = []
        }
        .fileImporter(isPresented: $picking, allowedContentTypes: [.commaSeparatedText, .tabSeparatedText, .plainText, .text]) { result in
            switch result {
            case .success(let url):
                let ok = url.startAccessingSecurityScopedResource()
                defer { if ok { url.stopAccessingSecurityScopedResource() } }
                guard let data = try? Data(contentsOf: url), data.count <= 2 * 1024 * 1024 else {
                    error = "That file could not be read, or is over 2 MB."
                    return
                }
                text = String(decoding: data, as: UTF8.self)
            case .failure(let e): error = errorText(e)
            }
        }
    }

    private func syncMapping(_ table: [[String]]) {
        guard let header = table.first else { return }
        if mapping.count != header.count { mapping = header.map(ImportParse.guess) }
    }

    private func rows() -> [[String: String]] {
        let table = ImportParse.parseDelimited(text)
        if ImportParse.isSpreadsheet(table) {
            syncMapping(table)
            return ImportParse.mapRows(table, mapping)
        }
        return text.split(whereSeparator: \.isNewline).compactMap { ImportParse.line(String($0)) }
    }

    private func run(dryRun: Bool) async {
        var list = rows()
        if list.isEmpty {
            error = "Nothing to import: no line reads as RANK Last, First."
            return
        }
        if !dryRun, let p = preview {
            // The client leaves the error rows out, as the web does.
            let bad = Set(p.rows.filter { $0.action == "error" }.map(\.index))
            list = list.enumerated().filter { !bad.contains($0.offset) }.map(\.element)
        }
        busy = true
        defer { busy = false }
        do {
            let body: [String: Any] = ["rows": list, "unit_id": store.unitId,
                                       "include_duplicates": includeDuplicates, "dry_run": dryRun]
            if dryRun {
                preview = try await app.api.post("personnel/import", body)
                error = nil
            } else {
                let r: ImportResult = try await app.api.post("personnel/import", body)
                Haptics.success()
                done = "Added \(r.added)" + (r.skippedDuplicates > 0 ? ", skipped \(r.skippedDuplicates) duplicate\(r.skippedDuplicates == 1 ? "" : "s")." : ".")
                preview = nil
                error = nil
                await store.load()
                await app.refreshUnits()
            }
        } catch { self.error = errorText(error) }
    }
}

struct ImportPreview: Decodable {
    struct Row: Decodable {
        var index: Int
        var rank: String?
        var last: String?
        var first: String?
        var unitName: String?
        var unitLabel: String?
        var unitMatched: Bool?
        var error: String?
        var action: String
    }
    struct Counts: Decodable { var add: Int; var duplicate: Int; var error: Int }
    var rows: [Row]
    var counts: Counts
}

struct ImportResult: Decodable {
    var added: Int
    var skippedDuplicates: Int
}

/// `parseDelimited()`, `guessImportColumn()`, `mapImportRows()` and
/// `parseImportLine()` from index.html, ported.
enum ImportParse {
    static let targets: [(String, String)] = [
        ("", "— ignore —"), ("rank", "Rank"), ("last", "Last name"), ("first", "First name"),
        ("name", "Name (Last, First)"), ("unit", "Unit / section"),
        ("phone", "Phone"), ("email", "Email"), ("dod_id", "DoD ID"), ("dob", "Date of birth"),
        ("mos", "MOS"), ("section", "Section (profile field)"), ("clearance", "Clearance"),
        ("date_of_rank", "Date of rank"), ("ets_date", "ETS date"),
        ("emergency_name", "Emergency contact"), ("emergency_phone", "Emergency phone"),
        ("next_of_kin", "Next of kin"), ("spouse_dependents", "Spouse / dependents"),
        ("address_street", "Street"), ("address_street2", "Street 2"), ("address_city", "City"),
        ("address_state", "State"), ("address_zip", "ZIP"),
        ("medical_date", "Medical date"), ("dental_date", "Dental date"),
    ]
    static let aliases: [String: String] = [
        "grade": "rank", "rnk": "rank", "lastname": "last", "lname": "last", "surname": "last",
        "firstname": "first", "fname": "first", "givenname": "first",
        "fullname": "name", "soldier": "name", "lastfirst": "name", "namelastfirst": "name",
        "section": "unit", "squad": "unit", "platoon": "unit", "team": "unit",
        "cell": "phone", "mobile": "phone", "cellphone": "phone", "phonenumber": "phone",
        "emailaddress": "email", "dodid": "dod_id", "edipi": "dod_id", "dateofbirth": "dob",
        "dor": "date_of_rank", "ets": "ets_date",
    ]

    private static func squash(_ s: String) -> String {
        String(s.lowercased().unicodeScalars.filter { CharacterSet.lowercaseLetters.contains($0) && $0.isASCII || CharacterSet.decimalDigits.contains($0) && $0.isASCII })
    }

    static func parseDelimited(_ text: String) -> [[String]] {
        var src = text
        if src.hasPrefix("\u{FEFF}") { src.removeFirst() }
        let firstLine = src.split(whereSeparator: \.isNewline).first { !$0.trimmingCharacters(in: .whitespaces).isEmpty } ?? ""
        let delim: Character = firstLine.contains("\t") ? "\t" : ","
        var rows: [[String]] = []
        var row: [String] = [], cell = "", quoted = false
        func endRow() {
            row.append(cell); cell = ""
            if row.contains(where: { !$0.trimmingCharacters(in: .whitespaces).isEmpty }) { rows.append(row) }
            row = []
        }
        let chars = Array(src)
        var i = 0
        while i < chars.count {
            let ch = chars[i]
            if quoted {
                if ch != "\"" { cell.append(ch) }
                else if i + 1 < chars.count && chars[i + 1] == "\"" { cell.append("\""); i += 1 }
                else { quoted = false }
            } else if ch == "\"" && cell.trimmingCharacters(in: .whitespaces).isEmpty {
                quoted = true; cell = ""
            } else if ch == delim {
                row.append(cell); cell = ""
            } else if ch == "\n" || ch == "\r" || ch == "\r\n" {
                endRow()
            } else {
                cell.append(ch)
            }
            i += 1
        }
        endRow()
        return rows
    }

    static func guess(_ header: String) -> String {
        let key = squash(header)
        guard !key.isEmpty else { return "" }
        if let a = aliases[key] { return a }
        return targets.first { $0.0 != "" && $0.0 != "section" && (squash($0.0) == key || squash($0.1) == key) }?.0 ?? ""
    }

    static func isSpreadsheet(_ table: [[String]]) -> Bool {
        guard let header = table.first, header.count >= 2 else { return false }
        let g = header.map(guess)
        return g.contains("name") || (g.contains("last") && g.contains("first"))
    }

    static func mapRows(_ table: [[String]], _ mapping: [String]) -> [[String: String]] {
        table.dropFirst().map { cells in
            var row: [String: String] = [:]
            for (i, target) in mapping.enumerated() {
                let v = i < cells.count ? cells[i].trimmingCharacters(in: .whitespaces) : ""
                guard !target.isEmpty, !v.isEmpty else { continue }
                if target == "name" {
                    if let comma = v.firstIndex(of: ",") {
                        row["last"] = v[..<comma].trimmingCharacters(in: .whitespaces)
                        row["first"] = v[v.index(after: comma)...].trimmingCharacters(in: .whitespaces)
                    } else {
                        let parts = v.split(whereSeparator: \.isWhitespace)
                        row["last"] = parts.first.map(String.init) ?? ""
                        row["first"] = parts.dropFirst().joined(separator: " ")
                    }
                } else {
                    row[target] = v
                }
            }
            return row
        }
        .filter { !$0.isEmpty }
    }

    /// "RANK Last, First" or "RANK Last First". The server decides whether the
    /// rank and names are good (it reads more rank spellings than the web paste did).
    static func line(_ raw: String) -> [String: String]? {
        let line = raw.trimmingCharacters(in: .whitespaces)
        guard !line.isEmpty, let space = line.firstIndex(where: \.isWhitespace) else { return nil }
        let rank = String(line[..<space])
        let rest = line[space...].trimmingCharacters(in: .whitespaces)
        if let comma = rest.firstIndex(of: ",") {
            return ["rank": rank, "last": rest[..<comma].trimmingCharacters(in: .whitespaces),
                    "first": rest[rest.index(after: comma)...].trimmingCharacters(in: .whitespaces)]
        }
        let parts = rest.split(whereSeparator: \.isWhitespace)
        guard parts.count >= 2 else { return nil }
        return ["rank": rank, "last": String(parts[0]), "first": parts.dropFirst().joined(separator: " ")]
    }
}

// MARK: Profile

struct AccountProfileView: View {
    @Environment(AppModel.self) private var app
    @State private var manage = false

    var body: some View {
        Form {
            if let me = app.me {
                Section("Profile") {
                    LabeledContent("Name", value: me.displayName)
                    LabeledContent("Email", value: me.email ?? "")
                    LabeledContent("Role", value: me.isOwner ? "Owner" : "Leader")
                    LabeledContent("Unit", value: me.unitName ?? "")
                }
            }
            Section {
                Button("Manage account", systemImage: "person.badge.key") { manage = true }
            } header: {
                Text("Sign-in & security")
            } footer: {
                Text("Name, email, password and two-factor sign-in.")
            }
        }
        .navigationTitle("Profile")
        .sheet(isPresented: $manage) {
            UserProfileView()
        }
    }
}

// MARK: Accounts & invites

struct AccountsView: View {
    @Environment(AppModel.self) private var app
    @State private var users: [Account] = []
    @State private var invites: [Invite] = []
    @State private var editing: Account?
    @State private var inviting = false
    @State private var error: String?
    @State private var query = ""

    var body: some View {
        List {
            Section {
                Button("Invite leader", systemImage: "person.badge.plus") { inviting = true }
            } footer: {
                Text(app.me?.isOwner == true
                     ? "Deactivating pauses app access; it does not cancel billing."
                     : "You can edit leader access within your unit and its sub-units. Only an owner can deactivate or remove access.")
            }
            Section("Leader accounts") {
                ForEach(filteredUsers) { u in
                    Button {
                        if u.editable == true { editing = u }
                    } label: {
                        HStack {
                            VStack(alignment: .leading, spacing: 2) {
                                Text(u.displayName).foregroundStyle(.primary)
                                Text([u.email ?? "", "\(u.isOwner ? "Owner" : "Leader") · \(u.unitName ?? "")"]
                                        .filter { !$0.isEmpty }.joined(separator: "\n"))
                                    .font(.caption).foregroundStyle(.secondary)
                            }
                            Spacer()
                            if u.isActive == false {
                                Text("Inactive").font(.caption).foregroundStyle(.orange)
                            }
                            if u.editable == true {
                                Image(systemName: "chevron.right").font(.caption).foregroundStyle(.tertiary)
                            }
                        }
                    }
                    .tint(.primary)
                }
            }
            Section {
                if invites.isEmpty { Text("No invites.").foregroundStyle(.secondary) }
                ForEach(invites) { i in
                    VStack(alignment: .leading, spacing: 2) {
                        HStack {
                            Text(i.label?.isEmpty == false ? i.label! : "(unnamed)")
                            Spacer()
                            Text((i.status ?? "").capitalized).font(.caption)
                                .foregroundStyle(i.status == "pending" ? .blue : .secondary)
                        }
                        Text("\(i.role == "owner" ? "Owner" : "Leader") at \(i.unitName ?? "") · by \(i.createdBy ?? "")")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    .swipeActions {
                        if i.status == "pending" {
                            Button("Revoke", role: .destructive) { Task { await revoke(i) } }
                        }
                    }
                }
            } header: {
                Text("Invites")
            } footer: {
                Text("An invite link works once and expires after 7 days. Swipe a pending one to revoke it.")
            }
            if let error { Section { Text(error).foregroundStyle(.red) } }
        }
        .navigationTitle("Accounts & invites")
        .searchable(text: $query, prompt: "Name, email or unit")
        .task { await load() }
        .refreshable { await load() }
        .sheet(item: $editing, onDismiss: { Task { await load() } }) { AccountEditSheet(account: $0) }
        .sheet(isPresented: $inviting, onDismiss: { Task { await load() } }) { InviteSheet() }
    }

    private var filteredUsers: [Account] {
        let q = query.trimmingCharacters(in: .whitespaces).lowercased()
        guard !q.isEmpty else { return users }
        return users.filter {
            [$0.displayName, $0.email ?? "", $0.unitName ?? ""].contains { $0.lowercased().contains(q) }
        }
    }

    private func load() async {
        do {
            async let u: [Account] = app.api.get("users")
            async let i: [Invite] = app.api.get("invites")
            (users, invites) = try await (u, i)
            error = nil
        } catch { self.error = errorText(error) }
    }

    private func revoke(_ i: Invite) async {
        do {
            try await app.api.delete("invites/\(i.token)")
            invites.removeAll { $0.token == i.token }
        } catch { self.error = errorText(error) }
    }
}

private struct AccountEditSheet: View {
    @Environment(AppModel.self) private var app
    @Environment(\.dismiss) private var dismiss
    let account: Account
    @State private var role = "leader"
    @State private var unitId = 0
    @State private var active = true
    @State private var confirmRemove = false
    @State private var error: String?
    @State private var saving = false

    var body: some View {
        let me = app.me
        let rootId = me?.rootId ?? 0
        let isSelf = account.id == me?.id
        NavigationStack {
            Form {
                Section {
                    LabeledContent("Name", value: account.displayName)
                    if let e = account.email, !e.isEmpty { LabeledContent("Email", value: e) }
                }
                Section {
                    Picker("Unit", selection: $unitId) {
                        ForEach(app.tree.subtree(of: me?.unitId ?? 0), id: \.self) { id in
                            Text(app.tree.unit(id)?.name ?? "Unit \(id)").tag(id)
                        }
                    }
                    if me?.isOwner == true {
                        Picker("Role", selection: $role) {
                            Text("Leader").tag("leader")
                            Text("Owner").tag("owner")
                        }
                    }
                } footer: {
                    Text("Owner is a role at the top of the organization only; a leader can be in any unit.")
                }
                if account.canToggleActive == true {
                    Section {
                        Toggle("Account active", isOn: $active)
                    } footer: {
                        Text("Deactivating pauses access; it does not cancel billing.")
                    }
                }
                if me?.isOwner == true && !isSelf {
                    Section {
                        Button("Remove account", role: .destructive) { confirmRemove = true }
                    }
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
            }
            .navigationTitle("Edit access")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { Task { await save(rootId: rootId) } }.disabled(saving)
                }
            }
            .confirmationDialog("Remove \(account.displayName)?", isPresented: $confirmRemove, titleVisibility: .visible) {
                Button("Remove", role: .destructive) { Task { await remove() } }
            } message: {
                Text("They lose access to the organization. Any subscription of theirs is cancelled.")
            }
            .onAppear {
                role = account.role ?? "leader"
                unitId = account.unitId ?? (me?.unitId ?? 0)
                active = account.isActive ?? true
            }
        }
    }

    private func save(rootId: Int) async {
        var body: [String: Any] = [:]
        if unitId != account.unitId { body["unit_id"] = unitId }
        if role != (account.role ?? "leader") { body["role"] = role }
        if active != (account.isActive ?? true) { body["is_active"] = active }
        guard !body.isEmpty else { dismiss(); return }
        if role == "owner" && unitId != rootId {
            error = "An owner sits at the top of the organization. Put them there, or make them a leader."
            return
        }
        saving = true
        defer { saving = false }
        do {
            try await app.api.send("PUT", "users/\(account.id)", body)
            Haptics.success()
            dismiss()
        } catch { self.error = errorText(error) }
    }

    private func remove() async {
        do {
            try await app.api.delete("users/\(account.id)")
            dismiss()
        } catch { self.error = errorText(error) }
    }
}

private struct InviteSheet: View {
    @Environment(AppModel.self) private var app
    @Environment(\.dismiss) private var dismiss
    @State private var label = ""
    @State private var unitId = 0
    @State private var role = "leader"
    @State private var link: String?
    @State private var error: String?
    @State private var saving = false

    struct Created: Decodable { var token: String; var url: String }

    var body: some View {
        let me = app.me
        NavigationStack {
            Form {
                if let link {
                    Section {
                        Text(link).font(.callout.monospaced()).textSelection(.enabled)
                        Button("Copy link", systemImage: "doc.on.doc") {
                            UIPasteboard.general.string = link
                            Haptics.success()
                        }
                        ShareLink(item: link) { Label("Share link", systemImage: "square.and.arrow.up") }
                    } header: {
                        Text("Invite link")
                    } footer: {
                        Text("Send it to \(label.isEmpty ? "them" : label). It works once and expires in 7 days.")
                    }
                } else {
                    Section {
                        TextField("Who is it for? (name)", text: $label)
                        Picker("Unit", selection: $unitId) {
                            ForEach(app.tree.subtree(of: me?.unitId ?? 0), id: \.self) { id in
                                Text(app.tree.unit(id)?.name ?? "Unit \(id)").tag(id)
                            }
                        }
                        if me?.isOwner == true && unitId == me?.rootId {
                            Picker("Role", selection: $role) {
                                Text("Leader").tag("leader")
                                Text("Owner").tag("owner")
                            }
                        }
                    } footer: {
                        Text("They sign in with the link and land in this unit.")
                    }
                }
                if let error { Section { Text(error).foregroundStyle(.red) } }
            }
            .navigationTitle("Invite leader")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button(link == nil ? "Cancel" : "Done") { dismiss() } }
                if link == nil {
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Create") { Task { await create() } }.disabled(saving)
                    }
                }
            }
            .onAppear { unitId = app.selectedUnitId ?? me?.unitId ?? 0 }
            .onChange(of: unitId) { if unitId != me?.rootId { role = "leader" } }
        }
    }

    private func create() async {
        saving = true
        defer { saving = false }
        do {
            let r: Created = try await app.api.post("invites", ["label": label, "unit_id": unitId, "role": role])
            link = r.url
            Haptics.success()
        } catch { self.error = errorText(error) }
    }
}

// MARK: Time zone

/// The organization's duty day: one zone per organization, owner-only to change.
struct TimeZoneView: View {
    @Environment(AppModel.self) private var app
    @State private var query = ""
    @State private var confirm: String?
    @State private var error: String?

    var body: some View {
        let current = app.timeZone.identifier
        let owner = app.me?.isOwner == true
        List {
            Section {
                LabeledContent("Duty day", value: current.replacingOccurrences(of: "_", with: " "))
            } footer: {
                Text(owner
                     ? "The roster rolls over, and absences start and end, at midnight in this zone, wherever a phone happens to be."
                     : "Only an owner can change the organization's time zone.")
            }
            if owner {
                Section("Change to") {
                    ForEach(zones, id: \.self) { z in
                        Button {
                            confirm = z
                        } label: {
                            HStack {
                                Text(z.replacingOccurrences(of: "_", with: " ")).foregroundStyle(.primary)
                                Spacer()
                                if z == current { Image(systemName: "checkmark").foregroundStyle(.blue) }
                            }
                        }
                        .tint(.primary)
                    }
                }
            }
            if let error { Section { Text(error).foregroundStyle(.red) } }
        }
        .navigationTitle("Time zone")
        .searchable(text: $query, prompt: "City or region")
        .confirmationDialog("Set the duty day to \(confirm ?? "")?", isPresented: Binding(
            get: { confirm != nil }, set: { if !$0 { confirm = nil } }), titleVisibility: .visible) {
            Button("Change time zone") { if let z = confirm { Task { await save(z) } } }
        } message: {
            Text("Every unit in the organization follows it.")
        }
    }

    private var zones: [String] {
        let all = TimeZone.knownTimeZoneIdentifiers
        let q = query.trimmingCharacters(in: .whitespaces).lowercased().replacingOccurrences(of: " ", with: "_")
        if q.isEmpty {
            return ["America/New_York", "America/Chicago", "America/Denver", "America/Phoenix",
                    "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu", "Europe/Berlin",
                    "Asia/Seoul", "Asia/Tokyo", "UTC"].filter { all.contains($0) || $0 == "UTC" }
        }
        return all.filter { $0.lowercased().contains(q) }
    }

    private func save(_ zone: String) async {
        guard let unit = app.selectedUnitId else { return }
        do {
            let s: UnitSettings = try await app.api.put("settings", ["timezone": zone], query: ["unit": String(unit)])
            app.adoptTimeZone(s.timezone ?? zone)
            Haptics.success()
            error = nil
        } catch { self.error = errorText(error) }
    }
}

// MARK: Backup & restore

struct BackupView: View {
    @Environment(AppModel.self) private var app
    @Environment(RosterStore.self) private var store
    @State private var exported: URL?
    @State private var busy = false
    @State private var picking = false
    @State private var pending: (name: String, json: [String: Any])?
    @State private var result: String?
    @State private var error: String?

    var body: some View {
        let owner = app.me?.isOwner == true
        Form {
            Section {
                if let exported {
                    ShareLink(item: exported) { Label("Save or send the backup file", systemImage: "square.and.arrow.up") }
                } else {
                    Button("Make a backup", systemImage: "externaldrive.badge.plus") { Task { await export() } }
                        .disabled(busy)
                }
            } header: {
                Text("Backup")
            } footer: {
                Text("A JSON file of everything in \(app.me?.unitName ?? "your unit") and below: units, soldiers, profiles, absences, duty, reports and settings. Save it to Files or send it to yourself.")
            }
            if owner {
                Section {
                    Button("Restore from a file…", systemImage: "arrow.counterclockwise") { picking = true }
                        .disabled(busy)
                } header: {
                    Text("Restore")
                } footer: {
                    Text("Replaces your whole organization with what is in the file. Only an owner can restore.")
                }
            }
            if busy { Section { ProgressView() } }
            if let result { Section { Text(result) } }
            if let error { Section { Text(error).foregroundStyle(.red) } }
        }
        .navigationTitle("Backup & restore")
        .fileImporter(isPresented: $picking, allowedContentTypes: [.json]) { r in
            switch r {
            case .success(let url):
                let ok = url.startAccessingSecurityScopedResource()
                defer { if ok { url.stopAccessingSecurityScopedResource() } }
                guard let data = try? Data(contentsOf: url),
                      let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                    error = "That file is not a Platoon Manager backup."
                    return
                }
                pending = (url.lastPathComponent, json)
            case .failure(let e): error = errorText(e)
            }
        }
        .confirmationDialog("Restore \(pending?.name ?? "")?", isPresented: Binding(
            get: { pending != nil }, set: { if !$0 { pending = nil } }), titleVisibility: .visible) {
            Button("Replace everything with this file", role: .destructive) {
                if let p = pending { Task { await restore(p.json) } }
            }
        } message: {
            Text("Your organization's soldiers, absences, duty, reports and settings are replaced by the file's. This cannot be undone; make a backup first.")
        }
    }

    private func export() async {
        busy = true
        defer { busy = false }
        do {
            let data = try await app.api.data("backup")
            let stamp = DayMath.string(Date(), in: app.timeZone)
            let url = FileManager.default.temporaryDirectory
                .appending(path: "platoon-backup-\(app.me?.unitSlug ?? "unit")-\(stamp).json")
            try data.write(to: url)
            exported = url
            error = nil
        } catch { self.error = errorText(error) }
    }

    private func restore(_ json: [String: Any]) async {
        busy = true
        defer { busy = false }
        do {
            let r = try await app.api.postObject("backup/restore", json)
            let people = r["personnel"] as? Int ?? 0
            let units = r["units_created"] as? Int ?? 0
            let skipped = r["skipped_rows"] as? Int ?? 0
            result = "Restored \(people) soldier\(people == 1 ? "" : "s"), \(units) unit\(units == 1 ? "" : "s") created"
                + (skipped > 0 ? ", \(skipped) row\(skipped == 1 ? "" : "s") skipped." : ".")
            Haptics.success()
            error = nil
            await app.refreshUnits()
            await store.load()
        } catch { self.error = errorText(error) }
    }
}
