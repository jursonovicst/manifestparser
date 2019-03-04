from io import BytesIO
import pycurl
# from lxml import etree
import numpy as np
import os
from typing import Callable
import chardet
# import random
import xml.etree.ElementTree as ET
import re
from urllib.parse import urlparse


class MParser:
    VIDEO = 'video'
    AUDIO = 'audio'
    SUBTITLE = 'txt'

    def __init__(self, url: str):

        self._url = url

        response = None

        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, self._url)
        response = BytesIO()
        curl.setopt(pycurl.WRITEDATA, response)
        curl.perform()

        if int(curl.getinfo(pycurl.HTTP_CODE)) != 200:
            curl.close()
            raise Exception(
                "Cannot load '%s', HTTP status code: %d" % (
                    curl.geturl(), curl.getinfo(pycurl.HTTP_CODE)))

        curl.close()

        # detect XML encoding and fuck Microsoft!
        charset = chardet.detect(response.getvalue())['encoding']

        # etree does not support default namespace, so remove default namespaces!
        manifest = re.sub(r'\sxmlns="[^"]+"', '', response.getvalue().decode(charset), count=1).encode(charset)
        self._manifest = ET.fromstring(manifest)

    @property
    def hss(self):
        if self._manifest.tag == 'SmoothStreamingMedia':
            return True

    @property
    def dash(self):
        if self._manifest.tag == "MPD":
            return True

    @property
    def live(self):
        if self.hss:
            return self._manifest.get('IsLive', default="false") == "true"

    @property
    def vod(self):
        if self.hss:
            return not self.live

    def bitrates(self, stream):
        bitrates = None
        if self.hss:
            streamindex = self._manifest.find("StreamIndex[@Type='%s']" % stream)
            if streamindex is None:
                return None

            bitrates = list(map(lambda element: int(element.get('Bitrate')), streamindex.findall('QualityLevel')))
            assert len(bitrates) == int(streamindex.get('QualityLevels')), "invalid bitrate count"

        elif self.dash:
            adaptationset = self._manifest.find("Period/AdaptationSet[@contentType='%s']" % stream)
            if adaptationset is None:
                return None

            bitrates = list(map(lambda element: int(element.get('bandwidth')), adaptationset.findall('Representation')))

        return bitrates

    def fragments(self, stream, strategy, lenght=0):
        assert isinstance(strategy, Callable), "Strategy must be callable: '%s'" % strategy

        if self.hss:
            # get the StreamIndex
            streamindex = self._manifest.find("StreamIndex[@Type='%s']" % stream)
            if streamindex is not None:

                # get TimeScale
                timescale = int(self._manifest.find("StreamIndex[@Type='%s']" % stream).get('TimeScale',
                                                                                            default=self._manifest.get(
                                                                                                'TimeScale',
                                                                                                default='10000000')))

                # get the fragment url part
                urltemplate = streamindex.get('Url')
                assert urltemplate is not None, "empty urltemplate"

                # get event times
                ds = list(map(lambda ee: int(ee.get('d')), streamindex.findall("c")))
                # add first fragment's timestamp
                ds.insert(int(streamindex.find('c').get('t', default='0')), 0)
                # duration of last fragment is not needed
                del ds[-1]
                assert len(ds) == int(streamindex.get('Chunks')), "fragment number mismatch: %d vs. %d" % (
                    len(ds), int(streamindex.get('Chunks')))

                bitrates = self.bitrates(stream)
                parser = urlparse(self._url)
                for cd in np.cumsum(ds):

                    # limit stream length
                    if lenght != 0 and float(cd) / timescale > lenght:
                        break

                    yield (float(cd) / timescale,
                           os.path.dirname(parser.path) + "/" + urltemplate.replace('{start time}', str(cd)).replace(
                               '{bitrate}', str(strategy(bitrates))),
                           (None, None)
                           )

        elif self.dash:
            # get the AdaptationSet

            bitrates = self.bitrates(stream)
            baseurls = {int(representation.get('bandwidth')): representation.find('BaseURL').text for representation in self._manifest.findall("Period/AdaptationSet[@contentType='%s']/Representation" % stream)}

            for at in range(100):

                # limit stream length
                if lenght != 0 and at > lenght:
                    break

                yield (at,
                       baseurls[strategy(bitrates)],
                       (None, None)
                       )


