from unittest import TestCase
from manifestparser import MParser
import http.server
import socketserver
from threading import Thread
import random


class MyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if 'Host' in self.headers and str.startswith(self.headers['Host'], "localhost"):
            self.send_response(302)
            self.send_header('Location', "http://127.0.0.1:%d%s" % (TestMParser.port, self.path))
            self.end_headers()
            self.wfile.write(b"")
        else:
            super().do_GET()


class TestMParser(TestCase):
    port = random.randint(20000, 60000)
    _handler = MyHandler
    _httpd = None
    _httpdthread = None

    @classmethod
    def setUpClass(cls):
        cls._httpd = socketserver.TCPServer(("", cls.port), cls._handler)
        cls._httpdthread = Thread(target=cls._httpd.serve_forever)
        cls._httpdthread.start()

    @classmethod
    def tearDownClass(cls):
        cls._httpd.shutdown()
        cls._httpdthread.join()

    def setUp(self):
        self.hssvod = MParser("http://127.0.0.1:%d/testdata/To_The_Limit_720.ism_Manifest" % self.port)
        self.dashvod = MParser(
            "http://127.0.0.1:%s/testdata/Jezebels_Reich-Main_Movie-9221571562371948872_v1_deu_20_1080k-HEVC-SD_HD_HEVC_DASH.mpd_streamProfile_Dash-NoText" % self.port)

    def test_redirect(self):
        hssvod = MParser("http://localhost:%d/testdata/To_The_Limit_720.ism_Manifest" % self.port)
        self.assertEqual(hssvod.urlp.netloc, "127.0.0.1:%d" % self.port)

    def test_hss(self):
        self.assertTrue(self.hssvod.hss)
        self.assertFalse(self.dashvod.hss)

    def test_dash(self):
        self.assertTrue(self.dashvod.dash)
        self.assertFalse(self.hssvod.dash)

    def test_vod(self):
        self.assertTrue(self.hssvod.vod)
        # self.assertTrue(self.dashvod.vod)

    def test_live(self):
        self.assertFalse(self.hssvod.live)
        # self.assertFalse(self.dashvod.live)

    def test_bitrates(self):
        self.assertListEqual(self.hssvod.bitrates(MParser.VIDEO),
                             [2962000, 2056000, 1427000, 991000, 688000, 477000, 331000, 230000])
        self.assertListEqual(self.hssvod.bitrates(MParser.AUDIO), [128000])

        self.assertListEqual(self.dashvod.bitrates(MParser.VIDEO),
                             [150363, 409396, 756951, 1046799, 2119631, 4111492, 5472034])
        self.assertListEqual(self.dashvod.bitrates(MParser.AUDIO), [128000])

    def test_fragments(self):
        values = list(self.hssvod.fragments(MParser.VIDEO, max, 30))
        self.assertEqual(len(values), 15)

        # values = list(self.dashvod.fragments(MParser.VIDEO, max, 30))
        # self.assertEqual(len(values), 10)
