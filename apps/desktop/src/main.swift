import Cocoa
import WebKit
import UniformTypeIdentifiers

final class AppDelegate: NSObject, NSApplicationDelegate, WKScriptMessageHandler {
    private var window: NSWindow!
    private var webView: WKWebView!
    private var serviceURL: URL!

    func applicationDidFinishLaunching(_ notification: Notification) {
        let frame = NSRect(x: 0, y: 0, width: 1180, height: 820)
        window = NSWindow(contentRect: frame, styleMask: [.titled, .closable, .miniaturizable, .resizable], backing: .buffered, defer: false)
        window.title = "声译 · 实时同传"
        window.minSize = NSSize(width: 850, height: 620)
        window.center()

        let address = ProcessInfo.processInfo.environment["SIMULMT_UI_URL"] ?? "http://127.0.0.1:8780"
        serviceURL = URL(string: address)
        let configuration = WKWebViewConfiguration()
        configuration.userContentController.add(self, name: "selectMedia")
        webView = WKWebView(frame: window.contentView!.bounds, configuration: configuration)
        webView.autoresizingMask = [.width, .height]
        window.contentView?.addSubview(webView)
        if let serviceURL {
            webView.load(URLRequest(url: serviceURL))
        }
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        guard message.name == "selectMedia" else { return }
        let panel = NSOpenPanel()
        panel.title = "选择要同传的音视频"
        panel.prompt = "导入"
        panel.allowedContentTypes = ["wav", "mp3", "m4a", "aac", "flac", "mp4", "mov", "m4v"].compactMap { UTType(filenameExtension: $0) }
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.beginSheetModal(for: window) { [weak self] response in
            guard response == .OK, let fileURL = panel.url else { return }
            self?.uploadMedia(fileURL)
        }
    }

    private func uploadMedia(_ fileURL: URL) {
        guard let serviceURL,
              var components = URLComponents(url: serviceURL, resolvingAgainstBaseURL: false) else {
            deliverMediaResult(["error": "无法连接同传服务"])
            return
        }
        components.path = "/api/media"
        components.queryItems = [URLQueryItem(name: "name", value: fileURL.lastPathComponent)]
        components.fragment = nil
        guard let endpoint = components.url else {
            deliverMediaResult(["error": "无法创建媒体导入请求"])
            return
        }
        var request = URLRequest(url: endpoint)
        request.httpMethod = "POST"
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        URLSession.shared.uploadTask(with: request, fromFile: fileURL) { [weak self] data, response, error in
            DispatchQueue.main.async {
                if let error {
                    self?.deliverMediaResult(["error": "媒体上传失败：\(error.localizedDescription)"])
                    return
                }
                let status = (response as? HTTPURLResponse)?.statusCode ?? 0
                let body = data.flatMap { try? JSONSerialization.jsonObject(with: $0) as? [String: Any] }
                guard (200..<300).contains(status), let path = body?["path"] as? String else {
                    let message = body?["message"] as? String ?? "媒体解码失败（HTTP \(status)）"
                    self?.deliverMediaResult(["error": message])
                    return
                }
                self?.deliverMediaResult(["path": path, "name": fileURL.lastPathComponent])
            }
        }.resume()
    }

    private func deliverMediaResult(_ result: [String: Any]) {
        guard let data = try? JSONSerialization.data(withJSONObject: result),
              let json = String(data: data, encoding: .utf8) else { return }
        webView.evaluateJavaScript("window.simulmtNativeMediaImport(\(json))")
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }
}

let application = NSApplication.shared
let delegate = AppDelegate()
application.delegate = delegate
application.setActivationPolicy(.regular)
application.run()
