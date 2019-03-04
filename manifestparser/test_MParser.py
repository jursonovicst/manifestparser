from unittest import TestCase
from manifestparser import MParser


class TestMParser(TestCase):
    def setUp(self):
        self.hssvod = MParser(
            "http://playready.directtaps.net/smoothstreaming/TTLSS720VC1/To_The_Limit_720.ism/Manifest")
        self.dashvod = MParser(
            "http://dash01.dmm.t-online.de/dash04/dashstream/streaming/mgm_serien/9221438342941160219/636480717292137630/Jezebels_Reich-Main_Movie-9221571562371948872_v1_deu_20_1080k-HEVC-SD_HD_HEVC_DASH.mpd?streamProfile=Dash-NoText")

    def test_hss(self):
        self.assertTrue(self.hssvod.hss)
        self.assertFalse(self.dashvod.hss)

    def test_dash(self):
        self.assertTrue(self.dashvod.dash)
        self.assertFalse(self.hssvod.dash)

    def test_vod(self):
        self.assertTrue(self.hssvod.vod)
        self.assertTrue(self.dashvod.vod)

    def test_live(self):
        self.assertFalse(self.hssvod.live)
        self.assertFalse(self.dashvod.live)

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

        values = list(self.dashvod.fragments(MParser.VIDEO, max, 30))
        self.assertEqual(len(values), 10)
