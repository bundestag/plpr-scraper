# coding: utf-8
import os
import re
import logging
import requests
from lxml import html
from urlparse import urljoin

log = logging.getLogger(__name__)


DATA_DIR = 'data'
TXT_DIR = os.path.join(DATA_DIR, 'txt')
OUT_DIR = os.path.join(DATA_DIR, 'out')

INDEX_URL = 'https://www.bundestag.de/plenarprotokolle'
ARCHIVE_URL = 'http://webarchiv.bundestag.de/archive/2013/0927/dokumente/protokolle/plenarprotokolle/plenarprotokolle/17%03.d.txt'

CHAIRS = [u'Vizepräsidentin', u'Vizepräsident', u'Präsident']

SPEAKER_STOPWORDS = ['ich zitiere', 'zitieren', 'Zitat', 'zitiert',
                     'ich rufe den', 'ich rufe die',
                     'wir kommen zur Frage', 'kommen wir zu Frage', 'bei Frage',
                     'fordert', 'fordern', u'Ich möchte',
                     'Darin steht', ' Aspekte ', ' Punkte ']

BEGIN_MARK = re.compile('Beginn: [X\d]{1,2}.\d{1,2} Uhr')
END_MARK = re.compile('\(Schluss:.\d{1,2}.\d{1,2}.Uhr\).*')
SPEAKER_MARK = re.compile('  (.{5,140}):\s*$')
TOP_MARK = re.compile('.*(rufe.*die Frage|zur Frage|Tagesordnungspunkt|Zusatzpunkt).*')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')


class SpeechParser(object):

    def __init__(self, lines):
        self.lines = lines
        self.missing_recon = False

    def parse_pois(self, group):
        for poi in group.split(' - '):
            text = poi
            speaker_name = None
            fingerprint = None
            sinfo = poi.split(': ', 1)
            if len(sinfo) > 1:
                speaker_name = sinfo[0]
                text = sinfo[1]
                speaker = speaker_name.replace('Gegenruf des Abg. ', '')
                fingerprint = 'XXX' + speaker
            yield (speaker_name, fingerprint, text)

    def __iter__(self):
        self.in_session = False
        speaker = None
        fingerprint = None
        in_writing = False
        chair_ = [False]
        text = []

        def emit(reset_chair=True):
            data = {
                'speaker': speaker,
                'in_writing': in_writing,
                'type': 'chair' if chair_[0] else 'speech',
                'fingerprint': fingerprint,
                'text': "\n\n".join(text).strip()
            }
            if reset_chair:
                chair_[0] = False
            [text.pop() for i in xrange(len(text))]
            return data

        for line in self.lines:
            # try:
            #     line = line.decode('latin-1')
            # except:
            #     pass
            line = line.replace(u'\u2014', '-')
            line = line.replace(u'\x96', '-')
            rline = line.replace(u'\xa0', ' ').strip()

            if not self.in_session and BEGIN_MARK.match(line):
                self.in_session = True
                continue
            elif not self.in_session:
                continue

            if END_MARK.match(rline):
                return

            if WRITING_BEGIN.match(rline):
                in_writing = True

            if WRITING_END.match(rline):
                in_writing = False

            if not len(line.strip()):
                continue

            is_top = False
            if TOP_MARK.match(rline):
                is_top = True

            has_stopword = False
            for sw in SPEAKER_STOPWORDS:
                if sw.lower() in line.lower():
                    has_stopword = True

            m = SPEAKER_MARK.match(line)
            if m is not None and not is_top and not has_stopword:
                if speaker is not None:
                    yield emit()
                _speaker = m.group(1)
                role = line.strip().split(' ')[0]
                fingerprint = 'YYY' + _speaker
                speaker = _speaker
                chair_[0] = role in CHAIRS
                continue

            m = POI_MARK.match(line)
            if m is not None:
                if not m.group(1).lower().strip().startswith('siehe'):
                    yield emit(reset_chair=False)
                    in_writing = False
                    for _speaker, _fingerprint, _text in self.parse_pois(m.group(1)):
                        yield {
                            'speaker': _speaker,
                            'in_writing': False,
                            'type': 'poi',
                            'fingerprint': _fingerprint,
                            'text': _text
                        }
                    continue

            text.append(line)
        yield emit()


def file_metadata(filename):
    fname = os.path.basename(filename)
    return int(fname[:2]), int(fname[2:5])


def parse_transcript(filename):
    wp, session = file_metadata(filename)
    fh = open(filename, 'rb')
    text = fh.read()
    try:
        text = text.decode('utf-8')
    except:
        text = text.decode('latin-1')
    text = text.replace('\r', '\n')
    
    # print wp, session, u'ü' in text

    # table = sl.get_table(engine, 'speech')
    # sio = find_local(url)
    # sample = {'source_etag': 'local'}
    # if sio is None:
    #     sample = sl.find_one(engine, table, source_url=url, matched=True)
    #     response, sio = fetch_stream(url)
    #     sample = check_tags(sample or {}, response, force)
    base_data = {
        'source_file': filename,
        'sitzung': session,
        'wahlperiode': wp
    }
    log.info("Loading transcript: %s/%s, from %s", wp, session, filename)
    print filename
    seq = 0
    parser = SpeechParser(text.split('\n'))
    for contrib in parser:
        contrib.update(base_data)
        contrib['sequence'] = seq
        # print contrib
        seq += 1

    print filename, seq
    #     if not len(contrib['text'].strip()):
    #         continue
    #     contrib.update(base_data)
    #     contrib['sequence'] = seq
    #     sl.upsert(engine, table, contrib, 
    #               unique=['source_url', 'sequence'])
    #     seq += 1
    # if not parser.missing_recon:
    #     sl.upsert(engine, table, {
    #                 'matched': True,
    #                 'source_url': url,
    #         }, unique=['source_url'])
    # else:
    #     raise InvalidReference()
    # return base_data


def fetch_protokolle():
    for d in TXT_DIR, OUT_DIR:
        try:
            os.makedirs(d)
        except:
            pass

    urls = set()
    res = requests.get(INDEX_URL)
    doc = html.fromstring(res.content)
    for a in doc.findall('.//a'):
        url = urljoin(INDEX_URL, a.get('href'))
        if url.endswith('.txt'):
            urls.add(url)

    for i in range(30, 260):
        url = ARCHIVE_URL % i
        urls.add(url)

    for url in urls:
        txt_file = os.path.join(TXT_DIR, os.path.basename(url))
        txt_file = txt_file.replace('-data', '')
        if os.path.exists(txt_file):
            continue

        r = requests.get(url)
        if r.status_code < 300:
            with open(txt_file, 'wb') as fh:
                fh.write(r.content)

            print url, txt_file


if __name__ == '__main__':
    fetch_protokolle()

    for filename in os.listdir(TXT_DIR):
        parse_transcript(os.path.join(TXT_DIR, filename))
