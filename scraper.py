# coding: utf-8
import os
import re
import logging
import requests
import dataset
from lxml import html
from urlparse import urljoin
from normality import normalize

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
                     'Darin steht', ' Aspekte ', ' Punkte ', 'Berichtszeitraum']

BEGIN_MARK = re.compile('Beginn: [X\d]{1,2}.\d{1,2} Uhr')
END_MARK = re.compile('(\(Schluss:.\d{1,2}.\d{1,2}.Uhr\).*|Schluss der Sitzung)')
SPEAKER_MARK = re.compile('  (.{5,140}):\s*$')
TOP_MARK = re.compile('.*(rufe.*die Frage|zur Frage|Tagesordnungspunkt|Zusatzpunkt).*')
POI_MARK = re.compile('\((.*)\)\s*$', re.M)
WRITING_BEGIN = re.compile('.*werden die Reden zu Protokoll genommen.*')
WRITING_END = re.compile(u'(^Tagesordnungspunkt .*:\s*$|– Drucksache d{2}/\d{2,6} –.*|^Ich schließe die Aussprache.$)')

POI_PREFIXES = re.compile(u'(Ge ?genruf|Weiterer Zuruf|Zuruf|Weiterer)( de[sr] (Abg.|Staatsministers|Bundesministers|Parl. Staatssekretärin))?')
NAME_REMOVE = re.compile(r'(\[.*\]|\(.*\)|^Abg.? |Liedvortrag|Bundeskanzler(in)?|CDU/? ?(CSU)?|SPD?|(, zur.*)|(, auf die)|( an die)|(, an .*)|(, Parl\. .*)|(gewandt)|(, Staatsmin.*)|(, Bundesmin.*)|(, Ministe.*))')
FP_REMOVE = re.compile('(^Dr.?( h.? ?c.?)?| (von( der)?)| [A-Z]\. )')

PARTIES_SPLIT = NAME_REMOVE = re.compile(r'(, (auf|an|zur|zum)( die| den )?(.* gewandt)?)')
PARTIES = {
    'cducsu': re.compile(' cdu ?(csu)?'),
    'spd': re.compile(' spd'),
    'linke': re.compile(' (die|der|den) linken?'),
    'fdp': re.compile(' fdp'),
    'gruene': re.compile(' bund ?nis\-?(ses)? ?90 die gru ?nen'),
}

eng = dataset.connect('sqlite:///data.sqlite')
table = eng['data']


def clean_text(fh):
    text = fh.read()
    try:
        text = text.decode('utf-8')
    except:
        text = text.decode('latin-1')
    text = text.replace('\r', '\n')
    text = text.replace(u'\xa0', ' ')
    text = text.replace(u'\x96', '-')
    text = text.replace(u'\u2014', '-')
    text = text.replace(u'\u2013', '-')
    return text


def clean_name(name):
    if name is None:
        return name
    name = POI_PREFIXES.sub('', name)
    name = NAME_REMOVE.sub('', name)
    name = name.strip('-')
    return name.strip()


def fingerprint(name):
    if name is None:
        return
    name = FP_REMOVE.sub(' ', name)
    return normalize(name).replace(' ', '-')


def in_america(you):  # can always find a party
    if you is None:
        return
    you = PARTIES_SPLIT.split(you)
    name = normalize(you[0])
    parties = set()
    for party, rex in PARTIES.items():
        if rex.findall(name):
            parties.add(party)
    if not len(parties):
        return
    parties = ':'.join(sorted(parties))
    return parties


class SpeechParser(object):

    def __init__(self, lines):
        self.lines = lines
        self.missing_recon = False

    def parse_pois(self, group):
        for poi in group.split(' - '):
            text = poi
            speaker_name = None
            sinfo = poi.split(': ', 1)
            if len(sinfo) > 1:
                speaker_name = sinfo[0]
                text = sinfo[1]
            yield (speaker_name, text)

    def __iter__(self):
        self.in_session = False
        speaker = None
        in_writing = False
        chair_ = [False]
        text = []

        def emit(reset_chair=True):
            data = {
                'speaker': speaker,
                'in_writing': in_writing,
                'type': 'chair' if chair_[0] else 'speech',
                'text': "\n\n".join(text).strip()
            }
            if reset_chair:
                chair_[0] = False
            [text.pop() for i in xrange(len(text))]
            return data

        for line in self.lines:
            line = line.strip()

            if not self.in_session and BEGIN_MARK.match(line):
                self.in_session = True
                continue
            elif not self.in_session:
                continue

            if END_MARK.match(line):
                return

            if WRITING_BEGIN.match(line):
                in_writing = True

            if WRITING_END.match(line):
                in_writing = False

            if not len(line.strip()):
                continue

            is_top = False
            if TOP_MARK.match(line):
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
                speaker = _speaker
                chair_[0] = role in CHAIRS
                continue

            m = POI_MARK.match(line)
            if m is not None:
                if not m.group(1).lower().strip().startswith('siehe'):
                    yield emit(reset_chair=False)
                    in_writing = False
                    for _speaker, _text in self.parse_pois(m.group(1)):
                        yield {
                            'speaker': _speaker,
                            'in_writing': False,
                            'type': 'poi',
                            'text': _text
                        }
                    continue

            text.append(line)
        yield emit()


def file_metadata(filename):
    fname = os.path.basename(filename)
    return int(fname[:2]), int(fname[2:5])


names = set()


def parse_transcript(filename):
    wp, session = file_metadata(filename)
    text = clean_text(open(filename, 'rb'))
    table.delete(wahlperiode=wp, sitzung=session)

    base_data = {
        'filename': filename,
        'sitzung': session,
        'wahlperiode': wp
    }
    print "Loading transcript: %s/%.3d, from %s" % (wp, session, filename)
    seq = 0
    parser = SpeechParser(text.split('\n'))

    for contrib in parser:
        contrib.update(base_data)
        contrib['sequence'] = seq
        contrib['speaker_cleaned'] = clean_name(contrib['speaker'])
        contrib['speaker_fp'] = fingerprint(contrib['speaker_cleaned'])
        contrib['speaker_party'] = in_america(contrib['speaker'])
        seq += 1
        table.insert(contrib)

    q = '''SELECT * FROM data WHERE wahlperiode = :w AND sitzung = :s
            ORDER BY sequence ASC'''
    fcsv = os.path.basename(filename).replace('.txt', '.csv')
    rp = eng.query(q, w=wp, s=session)
    dataset.freeze(rp, filename=fcsv, prefix=OUT_DIR, format='csv')


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
