from html2text import HTML2Text


HEADER_LABEL = [
    'header1',
    'header2',
    'header3',
    'header4',
    'header5',
    'header6',
]

def _handle_text_style(json_data_text, return_null=False):
    text = json_data_text.get('text', '')
    pure_text = text
    bold = json_data_text.get('bold')
    italic = json_data_text.get('italic')

    if italic:
        text = "_%s_" % text
    if bold:
        text = "**%s**" % text

    if (not text) and return_null:
        text = '.'
    return text, pure_text

# sdoc 2 html dom
# 1. header
def _handle_header_dom(header_json, header_type):
    output = ''
    for child in header_json['children']:
        if 'text' in child:
            output += child.get('text')
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)

    tag = {
        "header1": "<h1>%s</h1>",
        "header2": "<h2>%s</h2>",
        "header3": "<h3>%s</h3>",
        "header4": "<h4>%s</h4>",
        "header5": "<h5>%s</h5>",
        "header6": "<h6>%s</h6>",

    }.get(header_type)
    return tag % output

# 3 list including ordered / unordered list
def _handle_list_dom(list_json, tag='', ordered=False):
    for list_item in list_json['children']:
        item_eles = list_item['children']
        text = ''
        for lic in item_eles:

            if lic.get('type') == 'unordered_list':
                tag += _handle_list_dom(lic, '')
            if lic.get('type') == 'ordered_list':
                tag += _handle_list_dom(lic, '', True)

            if lic.get('type') == 'paragraph':
                for item in lic['children']:
                    if 'text' in item:
                        text += _handle_text_style(item)[0]
                    else:
                        item_type = item.get('type')
                        if item_type == 'link':
                            text_name = item['children'][0]['text']
                            text_url = item.get('href')
                            text += "<a href='%s'><span>%s</span></a>" % (text_url, text_name)
                tag += "<li><p><span>%s</span></p></li>" % text
    if ordered:
        res = "<ul>%s</ul>" % tag
    else:
        res = "<ul>%s</ul>" % tag
    return res

# 4 checkbox
def _handle_check_list_dom(check_list_json):
    output = ""
    checked = check_list_json.get('checked')
    for child in check_list_json['children']:
        if 'text' in child:
            output += _handle_text_style(child)[0]
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)

    if checked:
        output = "<p><span>* [x] %s</span></p>" % output
    else:
        output = "<p><span>* [ ] %s</span></p>" % output

    return output

# 5 blockquote
def _handle_blockquote_dom(blockquote_json):
    output = ""
    for child in blockquote_json['children']:
        child_type = child.get('type')
        if child_type in ['ordered_list', 'unordered_list']:
            output += _handle_list_dom(child, '', child_type == 'ordered_list')

        if child_type == 'link':
            text_name = child['children'][0]['text']
            text_url = child.get('href')
            output += "<a href='%s'><span>%s</span></a>" % (text_url, text_name)

        if child_type == 'paragraph':
            output += '%s' % _handle_pagragh_dom(child)

        if child_type == 'check_list_item':
            output += '%s' % _handle_check_list_dom(child)


        if 'text' in child:
            text = child.get('text')
            text_list = text.split("\n")
            output += ''.join(['<p><span>%s</span></p>' % t for t in text_list if t.strip()])

    tag = "<blockquote>%s</blockquote>" % output
    return tag

# 6 url link
def _handle_link_dom(link_json):
    href = link_json.get('href')
    link_child = link_json['children'][0]

    res = "<a href='%s'><span>%s</span></a>" % (href, link_child.get('text'))
    return res


# 7 pagragh
def _handle_pagragh_dom(pagragh_json):
    output = ''
    for child in pagragh_json['children']:
        if 'text' in child:
            output += _handle_text_style(child)[0]
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)


    result = "<p><span>%s</span></p>" % output
    return result.replace("\n", "")


def _handle_table_cell_dom(table_cell_json):
    output = ''
    for child in table_cell_json['children']:
        if 'text' in child:
            output += _handle_text_style(child)[0]
        else:
            child_type = child.get('type')
            if child_type == 'link':
                output += _handle_link_dom(child)

    return output


#  html2markdown
def handle_header(header_json, header_type):
    md_hander = HTML2Text(bodywidth=0) # no wrapping length
    dom = _handle_header_dom(header_json, header_type)
    return md_hander.handle(dom)


def handle_check_list(check_list_json):
    md_hander = HTML2Text(bodywidth=0) # no wrapping length
    return md_hander.handle(_handle_check_list_dom(check_list_json))


def handle_paragraph(paragraph_json):
    md_hander = HTML2Text(bodywidth=0) # no wrapping length
    dom = _handle_pagragh_dom(paragraph_json)
    return md_hander.handle(dom)


def handle_list(json_data, ordered=False):
    md_hander = HTML2Text(bodywidth=0) # no wrapping length
    html = _handle_list_dom(json_data, '', ordered)
    md = md_hander.handle(html)
    return md


def handle_codeblock(code_bloc_json):
    lang = code_bloc_json.get('language', '')
    output = ""
    for child in code_bloc_json.get('children'):
        if 'children' in child:
            output += "%s\n" % child.get('children', '')[0].get('text')
    return "```%s\n%s```" % (lang, output)


def handle_blockquote(json_data):
    md_hander = HTML2Text(bodywidth=0) # no wrapping length
    html = _handle_blockquote_dom(json_data)
    md = md_hander.handle(html)
    return md


def handle_table(table_json):
    md_hander = HTML2Text(bodywidth=0) # no wrapping length
    th_headers = ''
    th_body = ''
    first_table_row = table_json['children'][0]
    other_table_rows = table_json['children'][1:]

    for first_table_cell in first_table_row['children']:
        th_headers +=  "<th>%s</th>" % _handle_table_cell_dom(first_table_cell)

    for table_row in other_table_rows:
        td = ''
        for table_cell in table_row['children']:
            td += "<td>%s</td>" % _handle_table_cell_dom(table_cell)
        th_body += "<tr>%s</tr>" % td

    html = "<figure><table><thead><tr>%s</tr></thead><tbody>%s</tbody></table></figure>" % (th_headers, th_body)
    return md_hander.handle(html)


#
def json2md(json_data):
    doc_type = json_data.get('type')
    markdown_output = ''
    if doc_type == 'title':
        output = handle_header(json_data, 'header1')
        markdown_output += output

    if doc_type in HEADER_LABEL:
        output = handle_header(json_data, doc_type)
        markdown_output += output

    if doc_type == 'check_list_item':
        output = handle_check_list(json_data)
        markdown_output += output

    if doc_type == 'paragraph':
        output = handle_paragraph(json_data)
        markdown_output += output

    if doc_type == 'code_block':
        output = handle_codeblock(json_data)
        markdown_output += output

    if doc_type == 'table':
        output = handle_table(json_data)
        markdown_output += output

    if doc_type == 'unordered_list':
        output = handle_list(json_data)
        markdown_output += output

    if doc_type == 'ordered_list':
        output = handle_list(json_data, ordered=True)
        markdown_output += output

    if doc_type == 'blockquote':
        output = handle_blockquote(json_data)
        markdown_output += output
    return markdown_output

def sdoc2md(json_tree):
    results = []
    for sub in json_tree.get('children'):
        results.append(json2md(sub))
    markdown_text = "\n".join(results)
    return markdown_text
