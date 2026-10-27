import http.server
import socketserver
import socket
import os
import sys

PORT = 8000
DIRECTORY = os.path.dirname(os.path.abspath(__file__))

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def end_headers(self):
        # Enable CORS and Cache-Control headers
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        super().end_headers()

    def guess_type(self, path):
        if path.endswith('.json') or path.endswith('manifest.json'):
            return 'application/manifest+json'
        if path.endswith('.svg'):
            return 'image/svg+xml'
        if path.endswith('.js'):
            return 'application/javascript'
        return super().guess_type(path)

if __name__ == '__main__':
    # Ensure stdout handles UTF-8 on Windows
    if sys.platform.startswith('win'):
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

    local_ip = get_local_ip()
    handler = CustomHandler
    
    with socketserver.TCPServer(("0.0.0.0", PORT), handler) as httpd:
        print("\n" + "="*60)
        print("  SPOTIWAVE PWA - SERVEUR DE DEMONSTRATION")
        print("="*60)
        print(f"\n[PC] Ouvrir dans le navigateur :")
        print(f"   http://localhost:{PORT}")
        print(f"\n[iPhone] Meme Wi-Fi dans Safari :")
        print(f"   http://{local_ip}:{PORT}")
        print("\n" + "-"*60)
        print("Pour tester le mode PWA sur iPhone :")
        print("   1. Ouvrez l'adresse dans Safari")
        print("   2. Appuyez sur 'Partager'")
        print("   3. Choisissez 'Sur l'ecran d'accueil'")
        print("="*60 + "\n")
        sys.stdout.flush()
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nArret du serveur.")
