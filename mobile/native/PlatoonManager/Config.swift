import Foundation

/// Where the app points. Both values are public: the publishable key is what
/// https://app.platoonmanager.com/api/auth/config hands every browser.
enum Config {
    static let apiBase = URL(string: "https://app.platoonmanager.com")!
    static let clerkPublishableKey = "pk_live_Y2xlcmsucGxhdG9vbm1hbmFnZXIuY29tJA"
}
