import re
import logging
from transformers import AutoTokenizer

from seafevents.semantic_search.utils.constants import MODEL_VOCAB_PATH
logger = logging.getLogger(__name__)


tokenizer = AutoTokenizer.from_pretrained(MODEL_VOCAB_PATH)


class MarkdownHeaderTextSplitter:
    """Splitting markdown files based on specified headers."""

    def __init__(
        self, headers_to_split_on
    ):
        """Create a new MarkdownHeaderTextSplitter.

        Args:
            headers_to_split_on: Headers we want to track
        """
        # Given the headers we want to split on,
        # (e.g., "#, ##, etc") order by length
        self.headers_to_split_on = sorted(
            headers_to_split_on, key=lambda split: len(split[0]), reverse=True
        )

    def aggregate_lines_to_chunks(self, lines):
        """Combine lines with common metadata into chunks
        Args:
            lines: Line of text / associated header metadata
        """
        aggregated_chunks = []

        for line in lines:
            if (
                aggregated_chunks
                and aggregated_chunks[-1]["header_name"] == line["header_name"]
            ):
                # If the last line in the aggregated list
                # has the same metadata as the current line,
                # append the current content to the last lines's content
                aggregated_chunks[-1]["content"] += "  \n" + line["content"]
            else:
                # Otherwise, append the current line to the aggregated list
                aggregated_chunks.append(line)

        return aggregated_chunks

    def split_text(self, text):
        """Split markdown file
        Args:
            text: Markdown file"""

        # Split the input text by newline character ("\n").
        lines = text.split("\n")
        # Final output
        lines_with_metadata = []
        # Content and metadata of the chunk currently being processed
        current_content = []
        current_header_name = ''
        header_name = ''

        in_code_block = False
        headers = []
        for line in lines:
            stripped_line = line.strip()

            if not stripped_line:
                continue

            if stripped_line.startswith("```"):
                in_code_block = not in_code_block
                continue

            if in_code_block:
                continue

            # Check each line against each of the header types (e.g., #, ##)
            for sep in self.headers_to_split_on:
                # Check if line starts with a header that we intend to split on
                if stripped_line.startswith(sep) and (
                    # Header with no text OR header is followed by space
                    # Both are valid conditions that sep is being used a header
                    len(stripped_line) == len(sep)
                    or stripped_line[len(sep)] == " "
                ):
                    header_name = stripped_line[len(sep):].strip()
                    headers.append(header_name)

                    # Add the previous line to the lines_with_metadata
                    # only if current_content is not empty
                    if current_content:
                        lines_with_metadata.append(
                            {
                                "content": "\n".join(current_content),
                                "header_name": current_header_name,
                            }
                        )
                        current_content.clear()
                    break
            else:
                if stripped_line:
                    current_content.extend(headers)
                    current_content.append(stripped_line)
                    headers = []
                elif current_content:
                    lines_with_metadata.append(
                        {
                            "content": "\n".join(current_content),
                            "header_name": current_header_name,
                        }
                    )
                    current_content.clear()
            current_header_name = header_name

        # If it ends with a separate title, headers is not empty
        if headers:
            lines_with_metadata.append(
                {"content": "\n".join(headers), "header_name": current_header_name}
            )

        if current_content:
            lines_with_metadata.append(
                {"content": "\n".join(current_content), "header_name": current_header_name}
            )

        # lines_with_metadata has each line with associated header name
        # aggregate these into chunks based on common header name
        return self.aggregate_lines_to_chunks(lines_with_metadata)


def _split_text_with_regex(text, separator, keep_separator):
    # Now that we have the separator, split the text
    if separator:
        if keep_separator:
            # The parentheses in the pattern keep the delimiters in the result.
            _splits = re.split(f"({separator})", text)
            splits = [_splits[i - 1] + _splits[i] for i in range(1, len(_splits), 2)]
            splits = splits + _splits[-1:]
        else:
            splits = re.split(separator, text)
    else:
        splits = list(text)
    return [s for s in splits if s != ""]


def tokenizer_length(text):
    tokens = tokenizer.tokenize(text)
    return len(tokens)


class RecursiveCharacterTextSplitter(object):
    """Splitting text by recursively look at characters.

    Recursively tries to split by different characters to find one
    that works.
    """

    def __init__(
        self,
        separators=None,
        keep_separator: bool = True,
        is_separator_regex: bool = True,

        chunk_size: int = 4000,
        chunk_overlap: int = 200,
        length_function=len,
        strip_whitespace: bool = True
    ) -> None:
        """Create a new TextSplitter."""
        self._separators = separators or ["\n\n", "\n", " ", "。|！|？", "\.|\!|\?"]
        self._is_separator_regex = is_separator_regex

        if chunk_overlap > chunk_size:
            raise ValueError(
                f"Got a larger chunk overlap ({chunk_overlap}) than chunk size "
                f"({chunk_size}), should be smaller."
            )
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._length_function = length_function
        self._keep_separator = keep_separator
        self._strip_whitespace = strip_whitespace

    def _split_text(self, text: str, separators):
        """Split incoming text and return chunks."""
        final_chunks = []
        # Get appropriate separator to use
        separator = separators[-1]
        new_separators = []
        for i, _s in enumerate(separators):
            _separator = _s if self._is_separator_regex else re.escape(_s)
            if _s == "":
                separator = _s
                break
            if re.search(_separator, text):
                separator = _s
                new_separators = separators[i + 1:]
                break

        _separator = separator if self._is_separator_regex else re.escape(separator)
        splits = _split_text_with_regex(text, _separator, self._keep_separator)

        # Now go merging things, recursively splitting longer texts.
        _good_splits = []
        _separator = "" if self._keep_separator else separator
        for s in splits:
            if self._length_function(s) < self._chunk_size:
                _good_splits.append(s)
            else:
                if _good_splits:
                    merged_text = self._merge_splits(_good_splits, _separator)
                    final_chunks.extend(merged_text)
                    _good_splits = []
                if not new_separators:
                    final_chunks.append(s)
                else:
                    other_info = self._split_text(s, new_separators)
                    final_chunks.extend(other_info)
        if _good_splits:
            merged_text = self._merge_splits(_good_splits, _separator)
            final_chunks.extend(merged_text)
        return final_chunks

    def split_text(self, text):
        return self._split_text(text, self._separators)

    def _join_docs(self, docs, separator):
        if self._keep_separator:
            text = separator.join(docs)
        else:
            text = ''.join(docs)
        if self._strip_whitespace:
            text = text.strip()
        if text == "":
            return None
        else:
            return text

    def _merge_splits(self, splits, separator):
        # We now want to combine these smaller pieces into medium size
        # chunks to send to the LLM.
        separator_len = self._length_function(separator)

        docs = []
        current_doc = []
        total = 0
        for d in splits:
            _len = self._length_function(d)
            if (
                total + _len + (separator_len if len(current_doc) > 0 else 0)
                > self._chunk_size
            ):
                if total > self._chunk_size:
                    logger.warning(
                        f"Created a chunk of size {total}, "
                        f"which is longer than the specified {self._chunk_size}"
                    )
                if len(current_doc) > 0:
                    doc = self._join_docs(current_doc, separator)
                    if doc is not None:
                        docs.append(doc)
                    # Keep on popping if:
                    # - we have a larger chunk than in the chunk overlap
                    # - or if we still have any chunks and the length is long
                    while total > self._chunk_overlap or (
                        total + _len + (separator_len if len(current_doc) > 0 else 0)
                        > self._chunk_size
                        and total > 0
                    ):
                        total -= self._length_function(current_doc[0]) + (
                            separator_len if len(current_doc) > 1 else 0
                        )
                        current_doc = current_doc[1:]
            current_doc.append(d)
            total += _len + (separator_len if len(current_doc) > 1 else 0)
        doc = self._join_docs(current_doc, separator)
        if doc is not None:
            docs.append(doc)
        return docs
