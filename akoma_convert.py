# coding: utf-8
#
# Make some XML for great profit.
#
# cf.
# * http://examples.akomantoso.org/
# * http://examples.akomantoso.org/usage.html
# * http://examples.akomantoso.org/alphabetical.html
#

import os
import csv
import sys
from unicodecsv import DictReader
import unicodedata
from lxml import etree

DATA_PATH = os.environ.get('DATA_PATH', 'data')
XML_DIR = os.path.join(DATA_PATH, 'akomantoso')
OUT_DIR = os.path.join(DATA_PATH, 'out')

ROLES = {
    'chair': 'Sitzungsleitung',
    'speaker': 'Redner',
    'poi': 'Zwischenruf'
}

csv.field_size_limit(sys.maxsize)


def list_sessions():
    for path in os.listdir(OUT_DIR):
        yield os.path.join(OUT_DIR, path)


def iter_session(path):
    with open(path, 'rb') as fh:
        for row in DictReader(fh):
            yield row


def safe_text(text):
    text = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C")
    return text.strip()


def init_doc(row):
    root = etree.Element("akomaNtoso")
    debate = etree.SubElement(root, "debate")
    debate.set('name', 'hansard')

    meta = etree.SubElement(debate, "meta")
    preface = etree.SubElement(debate, "preface")
    title = etree.SubElement(preface, "docTitle")
    title.text = '%s. Sitzung des %s. Deutschen Bundestages' % \
        (row['sitzung'], row['wahlperiode'])
    number = etree.SubElement(preface, "docNumber")
    number.text = 'PlPr %s/%.3d' % (row['wahlperiode'], int(row['sitzung']))
    auth = etree.SubElement(preface, "docAuthority")
    auth.text = 'Deutscher Bundestag'
    legislature = etree.SubElement(preface, "legislature")
    legislature.text = '%s. Deutscher Bundestag' % row['wahlperiode']
    legislature.set('value', row['wahlperiode'])
    session = etree.SubElement(preface, "session")
    session.text = '%s. Sitzung' % row['sitzung']
    session.set('value', row['sitzung'])

    references = etree.SubElement(meta, "references")
    for role_id, title in ROLES.items():
        role = etree.SubElement(references, "TLCRole")
        role.set('id', role_id)
        role.set('href', '/ontology/role/bundestag.%s' % role_id)
        role.set('showAs', title)

    body = etree.SubElement(debate, "debateBody")
    return root, references, body


def convert_session(path):
    root, references, body = None, None, None
    prev_speaker, group = None, None
    fingerprints = {}

    for row in iter_session(path):
        if root is None:
            root, references, body = init_doc(row)

        speaker = row.get('speaker')
        fp = row.get('speaker_fp')

        poi = row.get('type') == 'poi'
        if not poi and not len(row.get('text').strip()):
            continue

        next_speaker = not poi and speaker and speaker != prev_speaker
        if group is None or next_speaker:
            group = etree.SubElement(body, "debateSection")
            group.set('id', 'group-%s' % row.get('sequence'))
            group.set('name', 'Rede')
            # group.set('as', '#%s' % row.get('type'))
            # if fp:
            #     group.set('by', '#%s' % fp)
            heading = etree.SubElement(group, "heading")
            heading.text = speaker
            prev_speaker = speaker

        if speaker:
            speech = etree.SubElement(group, "speech")
            speech.set('as', '#%s' % row.get('type'))
        else:
            speech = etree.SubElement(group, "scene")

        if fp:
            speech.set('by', '#%s' % fp)
            if fp not in fingerprints:
                person = etree.SubElement(references, "TLCPerson")
                person.set('id', fp)
                person.set('href', '/ontology/person/bundestag.%s' % fp)
                person.set('showAs', row.get('speaker_cleaned'))
                fingerprints[fp] = person

        if speaker:
            from_ = etree.SubElement(speech, "from")
            from_.text = speaker

        text = row.get('text') or ''
        for para in text.split('\n'):
            if len(para.strip()):
                p = etree.SubElement(speech, "p")
                p.text = safe_text(para)

    out_file = os.path.basename(path).replace('.csv', '.xml')
    out_file = os.path.join(XML_DIR, out_file)
    print [path, out_file]
    with open(out_file, 'wb') as fh:
        fh.write(etree.tostring(root, pretty_print=True,
                                encoding='utf-8',
                                xml_declaration=True))


if __name__ == '__main__':
    try:
        os.makedirs(XML_DIR)
    except:
        pass

    for infile in list_sessions():
        convert_session(infile)
        # sys.exit()
