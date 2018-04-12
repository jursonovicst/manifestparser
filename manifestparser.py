import pycurl
from StringIO import *
import xml.etree.ElementTree as ET
from urlparse import urlparse
import random


class SmoothStreamingMedia: #SmoothStreamingMedia

    @staticmethod
    def parsemanifest(url, proxy="", localip=None):
        cv = pycurl.Curl()
        cv.setopt(pycurl.FOLLOWLOCATION, True)
        cv.setopt(pycurl.USERAGENT, "manifestparser")
        cv.setopt(pycurl.SSL_VERIFYHOST, False)
        cv.setopt(pycurl.SSL_VERIFYPEER, False)
        cv.setopt(pycurl.PROXY, proxy)
        if localip is not None:
            cv.setopt(pycurl.INTERFACE, localip)

        buffer = StringIO()
        cv.setopt(cv.URL, url.rstrip())
        cv.setopt(pycurl.WRITEFUNCTION, buffer.write)
        cv.perform()

        retcode = int(cv.getinfo(pycurl.RESPONSE_CODE))

        if retcode >= 400:
            cv.close()
            raise Exception("Server returned %d, cannot fetch manifest: %s" %(retcode, url))

        cv.close()

        xml = ET.fromstring(buffer.getvalue())

        if xml.tag != "SmoothStreamingMedia":
            raise NotImplementedError("Smooth streaming tag %s has not been implemented" % xml.tag)

        smoothstreamingmedia = SmoothStreamingMedia(xml.get('MajorVersion',None), xml.get('MinorVersion',None), xml.get('TimeScale',10000000), xml.get('Duration',0), xml.get('IsLive',"false"), url.replace("/Manifest",""))

        for element in xml.iter("StreamIndex"):
            smoothstreamingmedia.addstreamelement(StreamElement.parsexml(element))

        return smoothstreamingmedia

    def __init__(self, majorversion, minorversion, timescale, duration, islive, baseurl):
        if majorversion is None or int(majorversion) != 2:
            raise Exception("Violation of 2.2.2.1: The major version of the Manifest Response message. MUST be set to 2. '%s' received" % majorversion)
        self.majorversion = int(majorversion)

        if minorversion is None or int(minorversion) not in [0,2]:
            raise Exception("Violation of 2.2.2.1: The minor version of the Manifest Response message. MUST be set to 0 or 2. '%s.%s' received" % (majorversion, minorversion))
        self.minorversion = int(minorversion)

        self.timescale = int(timescale)
        self.duration = int(duration)
        self.islive = True if str(islive).lower() in ['true'] else False

        self._streamelements = []
        self._baseurl = baseurl

    def addstreamelement(self, streamelement):
        self._streamelements.append(streamelement)

    def iterfragmenturls(self):
        for streamelement in self._streamelements:
            for url in streamelement.iterfragmenturls():
                yield self._baseurl + url

    def printtree(self):
        print("==========")
        print(self.__class__.__name__)
        print("majorv:\t%d" % self.majorversion)
        print("minorv:\t%d" % self.minorversion)
        print("timesc:\t%s" % str(self.timescale) if self.timescale is not None else "None")
        print("durati:\t%s" % str(self.duration) if self.duration is not None else "None")
        print("live stream" if self.islive else "on-demand stream")
        for streamelement in self._streamelements:
            streamelement.printtree()



class StreamElement:    #StreamIndex

    @staticmethod
    def parsexml(xml):
        if xml.tag != "StreamIndex":
            raise NotImplementedError("Invalid StreamIndex element: '%s'" % xml.tag)

        streamelement = StreamElement(xml.get('Type',None), xml.get('TimeScale',None), xml.get('Name',xml.attrib['Type']), xml.attrib['Url'])

        for element in xml.iter("QualityLevel"):
            streamelement.addtrackelement(TrackElement.parsexml(element))

        for element in xml.iter("c"):
            streamelement.addstreamfragment(StreamFragment.parsexml(element))

        return streamelement

    def __init__(self, type, streamtimescale, name, url):
        if( type is None or type not in ["video", "audio", "text"]):
            raise Exception("Violation of 2.2.2.3: The type of the stream: video, audio, or text. '%s' received" % type)
        self.type = str(type)

        self.streamtimescale = int(streamtimescale) if streamtimescale is not None else None
        self.name = str(name) if name is not None else None
        self.url = str(url)

        self._trackelements = []
        self._streamfragments = []

    def addtrackelement(self, trackelement):
        self._trackelements.append(trackelement)

    def addstreamfragment(self, streamfragment):
        self._streamfragments.append(streamfragment)

    def iterfragmenturls(self):
        for trackelement in self._trackelements:
            url = self.url.replace("{bitrate}", str(trackelement.bitrate))

            lastduration = 0
            starttime=0
            for streamfragment in self._streamfragments:
                if streamfragment.fragmenttime is not None:
                    starttime = streamfragment.fragmenttime
                else:
                    starttime += lastduration

                if (streamfragment.fragmentduration is not None):
                    lastduration = streamfragment.fragmentduration

                yield "/" + url.replace("{start time}", str(starttime))

    def printtree(self):
        print("==========")
        print(self.__class__.__name__)
        print("type:\t%s" % self.type)
        if (self.name is not None):
            print("name:\t%s" % self.name)
        if(self.url is not None):
            print("url:\t%s" % self.url)
        for trackelement in self._trackelements:
            trackelement.printtree()
        for streamfragment in self._streamfragments:
            streamfragment.printtree()



class TrackElement: #QualityLevel

    @staticmethod
    def parsexml(xml):
        if xml.tag != "QualityLevel":
            raise NotImplementedError("Invalid TrackElement element: '%s'" % xml.tag)

        return TrackElement(xml.get('Bitrate',None))

    def __init__(self, bitrate):
        if (bitrate is None):
            raise Exception("Violation of 2.2.2.5: The following fields are required and MUST be present in TrackAttributes: IndexAttribute and BitrateAttribute.")

        self.bitrate = int(bitrate)

    def printtree(self):
        print("--" + self.__class__.__name__ + "--")
        print("bitrt:\t%d" % self.bitrate)

class StreamFragment:

    @staticmethod
    def parsexml(xml):
        if xml.tag != "c":
            raise NotImplementedError("Invalid Fragment element: '%s'" % xml.tag)

        return StreamFragment(xml.get('t',None), xml.get('d',None))


    def __init__(self, fragmenttime, fragmentduration):
        if(fragmenttime is None and fragmentduration is None):
            raise Exception("Violation of 2.2.2.6: Either one or both of FragmentDuration and FragmentTime fields are required and MUST be present.")
        self.fragmenttime = int(fragmenttime) if fragmenttime is not None else None
        self.fragmentduration = int(fragmentduration) if fragmentduration is not None else None

    def printtree(self):
        print("--" + self.__class__.__name__ + "--")
        if (self.fragmenttime is not None):
            print("ftime:\t%d" % self.fragmenttime)
        if ( self.fragmentduration is not None):
            print("fdur:\t%d" % self.fragmentduration)






class ManifestParser:
    ST_OTHER = 0
    ST_DASH = 1
    ST_HSS = 2
    ST_HSSLIVE = 3

    @staticmethod
    def streamtypetostring(type):
        if type == ManifestParser.ST_DASH:
            return "Dash"
        elif type == ManifestParser.ST_HSS:
            return "HSS"
        elif type == ManifestParser.ST_HSSLIVE:
            return "HSS Live"

        return "Unknown"


    def __init__(self, proxy=""):
        self._url = None
        self._streamtype = ManifestParser.ST_OTHER
        self._baseurl = None
        self._manifest = None   # XMLTree element for the dash/hss Manifest file
        self._proxy = proxy     # proxy string


    def getstreamtype(self):
        return self._streamtype

    def getstreamtypestring(self):
        return ManifestParser.streamtypetostring(self.getstreamtype())

    # Returns true or false
    def fetchmanifest(self, url, localip=None):
        self._url = None
        self._streamtype = ManifestParser.ST_OTHER
        self._baseurl = None
        self._manifest = None

        self._cv = pycurl.Curl()
        self._cv.setopt(pycurl.FOLLOWLOCATION, True)
        self._cv.setopt(pycurl.USERAGENT, "manifestparser")
        self._cv.setopt(pycurl.SSL_VERIFYHOST, False)
        self._cv.setopt(pycurl.SSL_VERIFYPEER, False)
        self._cv.setopt(pycurl.PROXY, self._proxy)
        if localip is not None:
            self._cv.setopt(pycurl.INTERFACE, localip)

        buffer = StringIO()
        self._cv.setopt(self._cv.URL, url.rstrip())
        self._cv.setopt(pycurl.WRITEFUNCTION, buffer.write)
        self._cv.perform()

        retcode = int(self._cv.getinfo(pycurl.RESPONSE_CODE))

        if retcode >= 400:
            raise Exception("Server returned %d, cannot fetch manifest: %s" %(retcode, url))

        self._url = self._cv.getinfo(pycurl.EFFECTIVE_URL)
        self._cv.close()

        return self.parsemanifest(buffer.getvalue(), self._url)



    def parsemanifest(self, buff, url):
        self._url = url
        self._manifest = ET.fromstring(buff)

        if self._manifest.tag == "SmoothStreamingMedia":
            if self._manifest.get('MajorVersion','') != "2":
                raise NotImplementedError("Smooth streaming protocol version: %s.x is not implemented." % self._manifest.attrib['MajorVersion'])

            self._streamtype = ManifestParser.ST_HSS
            if self._manifest.get('IsLive', 'false').lower() == 'true':
                self._streamtype = ManifestParser.ST_HSSLIVE


        elif self._manifest.tag == "{urn:mpeg:DASH:schema:MPD:2011}MPD":
            if self._manifest.get('profiles') != 'urn:mpeg:dash:profile:isoff-on-demand:2011':
                raise NotImplementedError("MPEG-DASH profile: %s is not implemented." % self._manifest.attrib['profiles'])

            self._url = self._url.rstrip()
            self._streamtype = ManifestParser.ST_DASH
            d = urlparse(self._url)
            self._baseurl = d.scheme + "://" + d.netloc + '/'.join(d.path.split('/')[0:-1]) + '/'


        else:
            raise NotImplementedError("Unknown manifest: %s" % self._manifest.tag)

        return True


    def getduration(self):
        if self._streamtype == ManifestParser.ST_HSS:
            return int(self._manifest.get('Duration')) / self.gettimescale()
        elif self._streamtype == ManifestParser.ST_HSSLIVE:
            return None

        raise Exception("Not implemented for %s streams." % self.getstreamtypestring())


    # returns the timescale in local unit
    def gettimescale(self):
        if self._streamtype == ManifestParser.ST_HSS or self._streamtype == ManifestParser.ST_HSSLIVE:
            return int(self._manifest.get('TimeScale',10000000))    #HSS default value

        raise Exception("Not implemented for %s streams." % self.getstreamtypestring())

    def gettypes(self):
        if self._streamtype == ManifestParser.ST_HSS or self._streamtype == ManifestParser.ST_HSSLIVE:
            types = []

            for qlev in self._manifest.findall("./StreamIndex"):
                if str(qlev.attrib['Type']) not in types:
                    types.append(str(qlev.attrib['Type']))

            for type in types:
                yield type

        else:
            raise Exception("Not implemented for %s streams." % self.getstreamtypestring())

    def getbitratesfor(self, type="video"):
        if self._streamtype == ManifestParser.ST_HSS or self._streamtype == ManifestParser.ST_HSSLIVE:
            for qlev in self._manifest.findall("./StreamIndex[@Type='" + type + "']/QualityLevel"):
                yield int(qlev.attrib['Bitrate'])

        elif self._streamtype == ManifestParser.ST_DASH:
            ns = {'ns': 'urn:mpeg:DASH:schema:MPD:2011'}
            # for actor in self._root.findall(".//ns:ContentComponent[@contentType='"+contentType+"']../ns:Representation", ns):
            aset = self._manifest.find(".//ns:ContentComponent[@contentType='" + type + "']..", ns)
            if aset is None:
                raise Exception("No %s ContentComponent in manifest found" % type)
            for repr in aset.findall("./ns:Representation", ns):
                yield int(repr.attrib['bandwidth'])


    def getmaxbitratefor(self, type="video"):
        return max(i for i in self.getbitratesfor(type))

    def getminbitratefor(self, type="video"):
        return min(i for i in self.getbitratesfor(type))

    def getrndbitratefor(self, type="video"):
        bitrates = []
        for i in self.getbitratesfor(type):
            bitrates.append(i)
        return random.choice(bitrates)


    # returns a tuple of {url, byterange, fragmentlength} for all fragments
    def getfragmenturlsfor(self, bitrate, type="video"):
        if self._streamtype == ManifestParser.ST_HSS:
            timescale = self.gettimescale()
            baseurl = self._url.replace('/Manifest','') + '/' + self._manifest.find("./StreamIndex[@Type='" + type + "']").attrib['Url'].replace('{bitrate}',str(bitrate))

            t=0
            for c in self._manifest.findall("./StreamIndex[@Type='" + type + "']/c"):
                d = int(c.attrib['d'])
                yield {'url': baseurl.replace('{start time}',str(t)), 'byterange': None, 'time': t/timescale, 'duration': d / timescale}
                t += d

        elif self._streamtype == ManifestParser.ST_HSSLIVE:
            timescale = self.gettimescale()
            baseurl = self._url.replace('/Manifest', '') + '/' + self._manifest.find("./StreamIndex[@Type='" + type + "']").attrib['Url'].replace('{bitrate}', str(bitrate))

            t0 = None
            for c in self._manifest.findall("./StreamIndex[@Type='" + type + "']/c[@t]"):
                t = int(c.attrib['t'])
                if t0 is None:
                    t0 = t

                raise Exception("This should be implemented!!!")
                d=int(c.attrib['d'])
                yield {'url': baseurl.replace('{start time}', str(t)), 'byterange': None, 'time': (t - t0) / timescale, 'duration': d/ timescale}

                if c.attrib.has_key('d'):
                    while True:
                        yield {'url': baseurl.replace('{start time}', str(t)), 'byterange': None, 'time': (t - t0) / timescale, 'duration': d/ timescale}
                        t += d


        elif self._streamtype == ManifestParser.ST_DASH:     #TODO: check and rewrite
            mp4 = MP4Parser()
            repsegurl = mp.getrepsegurl(bitrate, type)
            repseg = mp.fetchdata(repsegurl)
            repurl = self._getrepurl(bitrate, type)
            offset = int(repsegurl['byterange'].split('-')[1])
            logging.debug(offset)
            for ret in mp4.getsidxsubsegments(repseg):
                yield {'url': repurl, 'byterange': "%d-%d" % (ret['from'], ret['to']), 'time': ret['duration']}


    # returns a tuple of {path, byterange, fragmentlength} for all fragments
    def getfragmentpathsfor(self, bitrate, type="video"):
        for ret in self.getfragmenturlsfor(bitrate, type):
            yield {'path': urlparse(ret['url']).path, 'byterange': ret['byterange'], 'time': ret['time'], 'duration': ret['duration']}


    def _getrepurl(self, bitrate, type="video"):  # TODO: type check not present
        url = ""
        if self._streamtype == ManifestParser.ST_DASH:
            ns = {'ns': 'urn:mpeg:DASH:schema:MPD:2011'}
            url = self._baseurl + self._manifest.find(".//ns:Representation[@bandwidth='" + str(bitrate) + "']/ns:BaseURL",ns).text
        return url

    def getrepsegurl(self, bitrate, type="video"):  # TODO: type check not present
        url = {'url': "", 'byterange': None}
        if self._streamtype == ManifestParser.ST_DASH:
            ns = {'ns': 'urn:mpeg:DASH:schema:MPD:2011'}
            url['byterange'] = \
                self._manifest.find(".//ns:Representation[@bandwidth='" + str(bitrate) + "']/ns:SegmentBase", ns).attrib[
                    'indexRange']
            url['url'] = self._getrepurl(bitrate)
        return url

    def getrepiniturl(self, bitrate):
        url = {'url': "", 'byterange': None}
        if self._streamtype == ManifestParser.ST_DASH:
            ns = {'ns': 'urn:mpeg:DASH:schema:MPD:2011'}
            url['byterange'] = \
                self._manifest.find(".//ns:Representation[@bandwidth='" + str(bitrate) + "']/ns:SegmentBase/ns:Initialization",
                                    ns).attrib['range']
            url['url'] = self._getrepurl(bitrate)
        return url


    def fetchdata(self, url):
        try:
            buffer = StringIO()
            self._cv.setopt(self._cv.URL, url['url'])
            self._cv.setopt(pycurl.WRITEFUNCTION, buffer.write)
            self._cv.setopt(pycurl.FOLLOWLOCATION, True)
            self._cv.setopt(pycurl.USERAGENT, "dashclient")
            if args.proxy != "-":
                self._cv.setopt(pycurl.PROXY, args.proxy)
                logging.debug("Using proxy " + args.proxy)
            else:
                self._cv.setopt(pycurl.PROXY, "")
            if url['byterange'] is not None:
                self._cv.setopt(pycurl.RANGE, url['byterange'])
            self._cv.setopt(pycurl.SSL_VERIFYHOST, False)
            self._cv.setopt(pycurl.SSL_VERIFYPEER, False)
            logging.info("Downloading fragment: " + url['url'] + " (bytes: " + url['byterange'] + ")")
            self._cv.perform()
        except TypeError:
            logging.error("wrong argument: " + url)
            return False
        except pycurl.error:
            logging.warning(self._cv.errstr() + ": " + url)
            return False

        if self._cv.getinfo(pycurl.RESPONSE_CODE) >= 300:
            logging.warning(self._cv.errstr() + ": " + url)
            return False

        return buffer

    def geturlsfor(self, type="video"):
        bw = []
        if self._streamtype == ManifestParser.ST_HSS:
            pass
        elif self._streamtype == ManifestParser.ST_DASH:
            ns = {'ns': 'urn:mpeg:DASH:schema:MPD:2011'}
            # for actor in self._root.findall(".//ns:ContentComponent[@contentType='"+contentType+"']../ns:Representation", ns):
            aset = self._manifest.find(".//ns:ContentComponent[@contentType='" + type + "']..", ns)
            if aset is None:
                logging.error("No %s ContentComponent in manifest found" % type)
                return []
            logging.debug("found %s ContentComponent (id=%s)" % (type, aset.find("./ns:ContentComponent", ns).attrib['id']))
            for repr in aset.findall("./ns:Representation", ns):
                bw.append(int(repr.attrib['bandwidth']))
                logging.debug(
                    "found " + type + " Representation (id=%s, bitrate=%s)" % (repr.attrib['id'], repr.attrib['bandwidth']))

        return bw
