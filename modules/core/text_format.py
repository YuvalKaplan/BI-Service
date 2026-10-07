"""
The cron email's format. Every section is plain text - also what the stages log and the scripts
print: a title line, "- " bullets (two more spaces of indent per level), pipe tables (table()) and
divider lines ("=====", "-----"), blocks separated by a blank line. to_html() renders the same
text for the email: a phone shows plain text in a proportional font, where no column lines up.
"""
import re
from html import escape

_DIVIDER = re.compile(r'^(=+|-+)$')
_BULLET = re.compile(r'^( *)- (.*)$')
_SEPARATOR_CELL = re.compile(r'^:?-+:?$')
_NUMBER = re.compile(r'^[-+$]?[\d.,/]+[%BMKT]?$|^-$')


def table(headers: list[str], rows: list[list[object]]) -> str:
    """A pipe table (Markdown), columns padded so it lines up in a monospace font."""
    cells = [[str(h) for h in headers]] + [['' if c is None else str(c).replace('|', '/') for c in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]

    def line(r: list[str]) -> str:
        return ('| ' + ' | '.join(c.ljust(w) for c, w in zip(r, widths))).rstrip() + ' |'

    return '\n'.join([line(cells[0]), '|' + '|'.join('-' * (w + 2) for w in widths) + '|', *(line(r) for r in cells[1:])])


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip('|').split('|')]


def _table_html(lines: list[str]) -> str:
    rows = [_cells(line) for line in lines]
    rows = [r for r in rows if not all(_SEPARATOR_CELL.match(c) for c in r)]
    header, body = rows[0], rows[1:]
    # Number columns (counts, percents, ranks) right-aligned.
    right = [bool(body) and all(i < len(r) and _NUMBER.match(r[i]) for r in body) for i in range(len(header))]

    def cell(tag: str, text: str, i: int) -> str:
        return f'<{tag}{" class=r" if i < len(right) and right[i] else ""}>{escape(text)}</{tag}>'

    head = ''.join(cell('th', h, i) for i, h in enumerate(header))
    rows_html = ''.join('<tr>' + ''.join(cell('td', c, i) for i, c in enumerate(r)) + '</tr>' for r in body)
    return f'<table><tr>{head}</tr>{rows_html}</table>'


# One style sheet, not per-cell styles: the cron email's tables run to hundreds of rows, and Gmail
# clips a message over ~100 KB of HTML.
_STYLE = ('body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.4}'
          'table{border-collapse:collapse;font-size:13px;margin:4px 0}'
          'th,td{text-align:left;padding:2px 6px;border-bottom:1px solid #ddd;vertical-align:top}'
          '.r{text-align:right}.t{font-weight:bold}.s{height:12px}'
          'hr{border:0;border-top:1px solid #ccc;margin:4px 0}hr.major{border-top:2px solid #888}'
          '.b0{padding-left:12px;text-indent:-12px}.b1{padding-left:28px;text-indent:-12px}'
          '.b2{padding-left:44px;text-indent:-12px}')


def to_html(text: str) -> str:
    """The email's HTML: a block's first line bold, bullets indented, pipe tables as tables,
    divider lines as rules ("=====" a heavier one than "-----")."""
    out: list[str] = []
    lines = text.split('\n')
    first_in_block = True
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            out.append('<div class=s></div>')
            first_in_block = True
        elif line.startswith('|'):
            start = i
            while i + 1 < len(lines) and lines[i + 1].startswith('|'):
                i += 1
            out.append(_table_html(lines[start:i + 1]))
            first_in_block = False
        elif _DIVIDER.match(line):
            out.append('<hr class=major>' if line.startswith('=') else '<hr>')
            first_in_block = True
        else:
            bullet = _BULLET.match(line)
            if bullet:
                depth = min(len(bullet.group(1)) // 2, 2)
                out.append(f'<div class=b{depth}>&bull; {escape(bullet.group(2))}</div>')
            elif first_in_block:
                out.append(f'<div class=t>{escape(line)}</div>')
            else:
                out.append(f'<div>{escape(line)}</div>')
            first_in_block = False
        i += 1
    return f'<html><head><style>{_STYLE}</style></head><body>{"".join(out)}</body></html>'
