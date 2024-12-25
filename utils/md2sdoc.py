import uuid
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode
from mdit_py_plugins.tasklists import tasklists_plugin
from mdit_py_plugins.dollarmath import dollarmath_plugin


def get_random_id():
    # - uuid.uuid4(): Generates a random UUID.
    # - .hex: Converts the UUID to a hexadecimal string representation.
    # - [:22]: Extracts the first 22 characters of the hexadecimal string.
    return uuid.uuid4().hex[:22]

# 将 Markdown 文档转换为 sdoc 文档
# 它使用了 MarkdownIt 库来解析 Markdown 文档（字符串转换成 sdoc 对象）
# 这里就是不同类型的转换和对应，关键是不同类型的递归和嵌套，以及不完整数据结构的异常处理，细节和 seafevents 核心逻辑无关

# 解析 Markdown 文档中的标记流，并将其转换为 sdoc 文档中的元素。
def parse_tokens(token_stream, **kwargs):
    empty_elem = {'id': get_random_id(), 'text': ''}

    sdoc_children = []
    for token in token_stream:
        if token.type == 'text':
            text = token.content
            sdoc_children.append({'id': get_random_id(), 'text': text, **kwargs})
        elif token.type == 'code_inline':
            text = token.content
            sdoc_children.append({'id': get_random_id(), 'text': text, 'code': True, **kwargs})
        elif token.type == 'em':
            sdoc_children.extend(parse_tokens(token.children, italic=True, **kwargs))
        elif token.type == 'strong':
            sdoc_children.extend(parse_tokens(token.children, bold=True, **kwargs))
        elif token.type == 'link':
            link_struct = {
                'id': get_random_id(),
                'type': 'link',
                'href': token.attrs.get('href'),
                'title': token.attrs.get('title'),
                'children': parse_tokens(token.children, **kwargs)
            }
            sdoc_children.extend([empty_elem, link_struct, empty_elem])
        elif token.type == 'image':
            img_struct = {
                'id': get_random_id(),
                'type': 'image',
                'children': [{'id': get_random_id(), 'text': ''}],
                'data': {'src': token.attrs.get('src')}
            }
            sdoc_children.extend([empty_elem, img_struct, empty_elem])
        elif token.type in {'html_block', 'html_inline'}:
            sdoc_children.extend(parse_html_inline_block(token.content))
        else:
            sdoc_children.append({'id': get_random_id(), 'text': token.content, **kwargs})
    return sdoc_children


# 解析标题（标题层级）
def parse_header(node):
    level_number = node.tag[1]
    inline_elem = node.children[0]
    sdoc_children = parse_tokens(inline_elem.children)
    return {
        'type': f'header{level_number}',
        'children': sdoc_children,
        'id': get_random_id()
    }


# 解析 Markdown 文档中的段落
def parse_paragraph(node):
    inline_elem = node.children[0]
    sdoc_children = parse_tokens(inline_elem.children)
    return {
        'type': 'paragraph',
        'children': sdoc_children,
        'id': get_random_id()
    }


# 解析 Markdown 文档中的数学公式
def parse_math(node):
    sdoc_children = [{'id': get_random_id(), 'text': node.content}]
    return {'type': 'paragraph', 'children': sdoc_children, 'id': get_random_id()}

# 解析列表
def parse_list(node, list_type):
    item_list = node.children
    children_list = []
    for item in item_list:
        parsed_item = {
            'id': get_random_id(),
            'type': 'list_item',
            'children': []
        }
        for child in item.children:
            if child.type == 'paragraph':
                inline_elem = child.children[0]
                parsed_item['children'].append({
                    'id': get_random_id(),
                    'type': 'paragraph',
                    'children': parse_tokens(inline_elem.children)
                })
            elif child.type in {'bullet_list', 'ordered_list'}:
                list_type_map = {'bullet_list': 'unordered_list'}.get(child.type, child.type)
                parsed_item['children'].append(parse_list(child, list_type_map))
        children_list.append(parsed_item)
    return {'type': list_type, 'id': get_random_id(), 'children': children_list}


# 勾选转换
def parse_check_list(node):
    children_list = []
    checked_status = False
    para_node = node.children[0].children[0]
    inline_node_list = para_node.children[0].children
    for inline in inline_node_list:
        # Get is checked
        if inline.type == 'html_inline':
            html = inline.content
            soup = BeautifulSoup(html, 'html.parser')
            input_tag = soup.find('input')
            if input_tag and input_tag.get('checked'):
                checked_status = True
        else:
            children_list.extend(parse_tokens([inline]))
    return {
        'type': 'check_list_item',
        'id': get_random_id(),
        'children': children_list,
        'checked': checked_status,
    }


# 行内代码转换
def parse_html_inline_block(html):
    empty_elem = {'id': get_random_id(), 'text': ''}
    element = ET.fromstring(html)

    # 转换成图片
    imgsrc = element.get('src')
    width = element.get('width')
    if imgsrc and width:
        img_struct = {
            'id': get_random_id(),
            'type': 'image',
            'children': [{'id': get_random_id(), 'text': ''}],
            'data': {'src': imgsrc, 'width': float(width)}
        }
        return [empty_elem, img_struct, empty_elem]
    else:
        return [empty_elem]


# 转换表格
def parse_table(node):
    children_list = []
    thead, tbody = node.children

    # 获取列数量
    column_count = len(thead.children[0].children)
    column_width = int(672 / column_count)

    # 创建基础表格
    table_sdoc = {
        'type': 'table',
        'id': get_random_id(),
        'children': children_list,
        'columns': [{'width': column_width}] * column_count
    }

    # 表格的行 = 表头 + 表体
    table_rows = thead.children + tbody.children

    # 遍历每一行
    for row in table_rows:
        row_children = row.children
        table_row_body = {
            'type': 'table_row',
            'id': get_random_id(),
            'children': [],
            'style': {'min_height': 43}
        }

        # 遍历单元格
        for cell in row_children:
            cell_content = parse_tokens(cell.children[0].children)
            table_cell = {
                'id': get_random_id(),
                'type': 'table_cell',
                'children': cell_content
            }
            table_row_body['children'].append(table_cell)

        children_list.append(table_row_body)

    return table_sdoc

def parse_codeblock(node):
    # 按照行切分
    '''
    Parse a markdown node of type 'fence' to a sdoc code_block element.
    :param node: The markdown-it node of type 'fence'
    :return: A sdoc code_block element
    '''
    code_lines = node.content.strip().split('\n')
    children_list = []
    # 处理每一行，转换成对象
    for line in code_lines:
        children_list.append({
            'id': get_random_id(),
            'type': 'code_line',
            'children': [{'id': get_random_id(), 'text': line}]
        })
    return {
        'id': get_random_id(),
        'type': 'code_block',
        'style': {'white_space': 'nowrap'},
        'language': node.info,
        'children': children_list,
    }


# 将 Markdown 文档转换为 sdoc 文档，主函数入口，其他都是解析不同类型的元素（实际上这部分应该单独封装一个库，不同场景使用）
# 未来如果增删某个模块，例如流程图，每一个转换器都需要改
def md2sdoc(md_txt, username=''):
    """
    将 Markdown 文档转换为 sdoc 文档
    通过 Markdown-It 解析 Markdown 文档，解析结果是一个 SyntaxTreeNode 对象，包含头、列表、段落、代码块、表格、图片等
    1. 头 - 3 层
    2. 列表 - 无序、有序
    3. 段落 - 1 层
    4. 代码块 - 1 层
    5. 表格 - 1 层
    6. 图片 - 1 层
    """
    # 使用任务列表插件，货币数学插件
    md = MarkdownIt("gfm-like").use(tasklists_plugin).use(dollarmath_plugin)
    # 解析 md 成 tokens
    tokens = md.parse(md_txt)
    # tokens 转换成语法树
    root = SyntaxTreeNode(tokens).children

    def _parse_node(root):
        children_list = []
        # 递归遍历语法树
        for node in root:
            if node.type == 'heading':
                children_list.append(parse_header(node))
            elif node.type == 'paragraph':
                children_list.append(parse_paragraph(node))
            elif node.type == 'math_block':
                children_list.append(parse_math(node))
            # 有序列表和无序列表
            elif node.type == 'bullet_list':
                if node.attrs.get('class') == 'contains-task-list':
                    children_list.append(parse_check_list(node))
                else:
                    children_list.append(parse_list(node, 'unordered_list'))
            elif node.type == 'ordered_list':
                children_list.append(parse_list(node, 'ordered_list'))
            elif node.type == 'table':
                children_list.append(parse_table(node))
            elif node.type == 'fence':
                children_list.append(parse_codeblock(node))
            # 块级元素
            elif node.type == 'blockquote':
                children_list.extend(
                    [
                        {
                            'id': get_random_id(),
                            'type': 'blockquote',
                            'children': _parse_node(node),
                        }
                    ]
                )
            # html 代码块
            elif node.type == 'html_block':
                children_list.append(
                    {
                        'type': 'paragraph',
                        'children': parse_html_inline_block(node.content),
                        'id': get_random_id(),
                    }
                )
        return children_list

    # 从根节点递归转换
    children_list = _parse_node(root)
    # 返回最终 sdoc 对象，包括其他属性
    sdoc_json = {
        'cursors': {},
        'last_modify_user': username,
        'elements': children_list,
        'version': 1,
        'format_version': 4,
    }
    return sdoc_json
