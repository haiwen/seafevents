import pypandoc
import json
import random
import string
import re
from seafevents.file_converter.utils import IMAGE_PATTERN


def get_random_id():
    ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 22))
    return ran_str


def parse_italic(italic_json, json_doc={}):
    json_doc['ITALIC'] = True
    children = italic_json['c']
    for item in children:
        if item['t'] == 'Strong':
            parse_strong(item, json_doc)
        if item['t'] == 'Str':
            json_doc['text'] = item['c']
            json_doc['id'] = get_random_id()
    return json_doc

def parse_strong(strong_json, json_doc={}):
    json_doc['BOLD'] = True
    children = strong_json['c']
    for item in children:
        if item['t'] == 'Emph':
            parse_italic(item, json_doc)
        if item['t'] == 'Str':
            json_doc['text'] = item['c']
            json_doc['id'] = get_random_id()

    return json_doc

def parse_plain(plain):
    l = []
    for item in plain['c']:
        if item['t'] == 'Str':
            l.append({'text': item['c'], 'id': get_random_id()})
        if item['t'] == 'Space':
            l.append({'text': ' ', 'id': get_random_id()})
        if item['t'] == 'Link':
            l.append(parse_link(item))
        if item['t'] == 'Code':
            l.append(parse_inline_code(item))
    return l


def parse_link(link_json):
    link_url = link_json['c'][2][0]
    link_main = link_json['c'][1]
    sdoc_json = {
        'type': 'link',
        'href': link_url,
        'children': [],
        'id': get_random_id(),
    }
    for item in link_main:
        if item['t'] == 'Str':
            sdoc_json['children'].append({'text': item['c'], 'id': get_random_id()})
            sdoc_json['title'] = item['c']

    return sdoc_json

def parse_header(header_json):
    header_level = header_json['c'][0]
    sdoc_json = {
        'type': 'header%s' % header_level,
        'children': [],
        'id':get_random_id()
    }
    header_structure = header_json['c'][2]
    for item in header_structure:
        if item['t'] == 'Str':
            sdoc_json['children'].append({'text': item['c'], "id": get_random_id()})
        if item['t'] == 'Space':
            sdoc_json['children'].append({'text': ' ', 'id': get_random_id()})
        if item['t'] == 'Link':
            sdoc_json['children'].append(parse_link(item))
        if item['t'] == 'Strong':
            sdoc_json['children'].append(parse_strong(item, {}))

    return sdoc_json

def parse_image(image_json):
    image_link = image_json['c'][2][0]
    sdoc_json = {
        'type': 'image',
        'children': [{'id': get_random_id(), 'text': ''}],
        'id': get_random_id(),
        'data': {'src': image_link},
    }
    return sdoc_json

def parse_raw_inline(inline_json):
    try:
        txt_type = inline_json['c'][0]
        if txt_type == 'html':
            img_html = inline_json['c'][1]
            image_link_res = re.findall(IMAGE_PATTERN, img_html)
            if image_link_res:
                image_link = image_link_res[0]
                sdoc_json = image_link and {
                    'type': 'image',
                    'children': [{'id': get_random_id(), 'text': ''}],
                    'id': get_random_id(),
                    'data': {'src': image_link},
                }
                return sdoc_json
    except:
        return None
    return None


def parse_inline_code(code_json):
    code_text = code_json['c'][-1]
    sdoc_json = {
        'text': code_text,
        'id': get_random_id()
    }
    return sdoc_json

def parse_list_sub(list_json):
    sdoc_json = {'type':'unordered_list', 'id': get_random_id(), 'children': []}
    for items in list_json['c']:
        list_item = {'type':'list-item', 'id': get_random_id(), 'children': []}
        for item in items:
            if item['t'] in ['Plain', 'Para']:
                list_item['children'].append({'type': 'list-lic', 'children': parse_plain(item), 'id': get_random_id()})
            if item['t'] == 'BulletList':
                list_item['children'].append(parse_list(item))
        sdoc_json['children'].append(list_item)
    return sdoc_json

def parse_list(list_json):
    sdoc_json = {'type':'unordered_list', 'id': get_random_id(), 'children': []}
    for items in list_json['c']:
        list_item = {'type':'list-item', 'id': get_random_id(), 'children': []}
        for item in items:
            if item['t'] in ['Plain', 'Para']:
                list_item['children'].append({'type': 'list-lic', 'children': parse_plain(item), 'id': get_random_id()})
            if item['t'] == 'BulletList':
                list_item['children'].append(parse_list_sub(item))
        sdoc_json['children'].append(list_item)
    return sdoc_json


def parse_codeblock(code_json):
    try:
        lang = code_json['c'][0][1][0]
    except:
        lang = ''

    sdoc_json = {
        'type': 'code-block',
        'children': [],
        'id': get_random_id(),
        'language': lang or 'plaintext',
        'style':{'white_space': "nowrap"}
    }
    main_code = code_json['c'][1]
    for code in main_code.split('\n'):
        sdoc_json['children'].append({
            'type':'code-line',
            'id': get_random_id(),
            'children':[{'text': code}]
        })
    return sdoc_json


def parse_paragragh(para_json):
    sdoc_json = {
        'type': 'paragraph',
        'children': [],
        'id': get_random_id()
    }
    for item in para_json['c']:
        if item['t'] == 'Str':
            sdoc_json['children'].append({'text': item['c'], "id": get_random_id()})
        if item['t'] == 'Space':
            sdoc_json['children'].append({'text': ' ', "id": get_random_id()})
        if item['t'] == 'Link':
            sdoc_json['children'].append(parse_link(item))
        if item['t'] == 'Strong':
            sdoc_json['children'].append(parse_strong(item, {}))
        if item['t'] == 'Image':
            sdoc_json['children'].append(parse_image(item))
        if item['t'] == 'Emph':
            sdoc_json['children'].append(parse_italic(item, {}))
        if item['t'] == 'RawInline':
            res = parse_raw_inline(item)
            if res:
                sdoc_json['children'].append(parse_raw_inline(item))

    return sdoc_json


def parse_blockquote(block_json):
    sdoc_json = {
        'type': 'blockquote',
        'children': [],
        'id': get_random_id()
    }
    for item in block_json['c']:
        if item['t'] == 'Para':
            sdoc_json['children'].append(parse_paragragh(item))
        if item['t'] == 'BulletList':
            sdoc_json['children'].append(parse_list(item))

    return sdoc_json

def parse_table(table_json):
    table_sdoc = {
        'type': 'table',
        'id': get_random_id(),
        'children': [],
        'columns': []
    }
    table_head = table_json['c'][3]
    column_num = len(table_head)
    column_length = int (672 / column_num)
    for i in range(column_num):
        table_sdoc['columns'].append({'width': column_length})

    table_row_head = {
        'type': 'table-row',
        'id': get_random_id(),
        'children': [],
        'style': {'minHeight': 43}
    }
    for row in table_head:
        table_cell = {
            'id': get_random_id(),
            'children': [],
            'type': 'table-cell'
        }
        if not row:
            row = [{'t': 'Plain', 'c': [{'t': 'Str', 'c': ''}]}]
        for c in row[0]['c']:
            if c['t'] == 'Str':
                table_cell['children'].append({'text': c['c'], 'id': get_random_id(), })
            if c['t'] == 'Space':
                table_cell['children'].append({'text': ' ', 'id': get_random_id(), })
            if c['t'] == 'Code':
                table_cell['children'].append(parse_inline_code(c))
            if c['t'] == 'Strong':
                table_cell['children'].append(parse_strong(c, {}))
            if c['t'] == 'Emph':
                table_cell['children'].append(parse_italic(c, {}))
            if c['t'] == 'Link':
                table_cell['children'].append(parse_link(c))
        table_row_head['children'].append(table_cell)

    table_sdoc['children'].append(table_row_head)

    table_body = table_json['c'][4]

    for row in table_body:
        table_row_body = {
            'type': 'table-row',
            'id': get_random_id(),
            'children': [],
            'style': {'minHeight': 43}
        }
        for v in row:
            table_cell = {
                'id': get_random_id(),
                'children': [],
                'type': 'table-cell'
            }

            if not v:
                v = [{'t': 'Plain', 'c': [{'t': 'Str', 'c': ''}]}]

            for c in v[-1]['c']:
                if c['t'] == 'Str':
                    table_cell['children'].append({'text': c['c'], 'id': get_random_id(), })
                if c['t'] == 'Space':
                    table_cell['children'].append({'text': ' ', 'id': get_random_id(), })
                if c['t'] == 'Strong':
                    table_cell['children'].append(parse_strong(c, {}))
                if c['t'] == 'Emph':
                    table_cell['children'].append(parse_italic(c, {}))
                if c['t'] == 'Link':
                    table_cell['children'].append(parse_link(c))
                if c['t'] == 'Code':
                    table_cell['children'].append(parse_inline_code(c))


            table_row_body['children'].append(table_cell)

        table_sdoc['children'].append(table_row_body)
    return table_sdoc


def md2sdoc(md_txt, username=''):
    md_ast = pypandoc.convert_text(md_txt, 'json', 'markdown')
    json_ast = json.loads(md_ast)
    blocks = json_ast['blocks']

    l = []
    for item in blocks:
        if item['t'] == 'Header':
            l.append(parse_header(item))

        if item['t'] == 'Para':
            l.append(parse_paragragh(item))

        if item['t'] == 'CodeBlock':
            l.append(parse_codeblock(item))

        if item['t'] == 'BulletList':
            l.append(parse_list(item))

        if item['t'] == 'BlockQuote':
            l.append(parse_blockquote(item))

        if item['t'] == 'Table':
            l.append(parse_table(item))

    sdoc_json = {
        'cursors': {},
        'last_modify_user': username,
        'children': l,
        'version': 1
    }

    return sdoc_json
