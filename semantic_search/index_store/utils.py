# -*- coding: utf-8 -*-
import os
import logging
from seafevents.semantic_search.index_store.extract import ExtractorFactory
from seafevents.semantic_search.config import SUPPORT_INDEX_FILE_TYPES

from seafobj import fs_mgr, commit_mgr

logger = logging.getLogger(__name__)


REPO_FILE_INDEX_CONTENT_LIMIT = 200


def get_document_add_params(embedding_api, sentences, index_name, path):
    add_params = []
    embeddings = embedding_api.embeddings(sentences)
    for item in embeddings['data']:
        index_info = {"index": {"_index": index_name}}
        vector_info = {
            "path": path,
            "vec": item['embedding'],
            "content": sentences[item['index']][:REPO_FILE_INDEX_CONTENT_LIMIT]
        }
        add_params.append(index_info)
        add_params.append(vector_info)
    return add_params


def parse_file_to_sentences(index_name, file_info, commit_id):
    path = file_info[0]
    obj_id = file_info[1]
    mtime = file_info[2]
    size = file_info[3]
    repo_id = index_name

    path_string, ext = os.path.splitext(path)
    if ext.lower() not in SUPPORT_INDEX_FILE_TYPES:
        return []

    sentences = [path_string]
    if size:
        new_commit = commit_mgr.load_commit(repo_id, 0, commit_id)
        version = new_commit.get_version()

        extractor = ExtractorFactory.get_extractor(os.path.basename(path))
        file_sentences = extractor.extract(repo_id, version, obj_id, path) if extractor else []
        if file_sentences:
            sentences.extend(file_sentences)

    return sentences


def rank_fusion(doc_lists, weights=None, c=60):
    """
    Args:
        doc_lists: A list of rank lists, where each rank list contains unique items.
        weights: A list of weights corresponding to the docs. Defaults to equal
            weighting for all docs.
        c: A constant added to the rank, controlling the balance between the importance
            of high-ranked items and the consideration given to lower-ranked items.
            Default is 60.

    Returns:
        list: The final aggregated list of items sorted by their weighted RRF
                scores in descending order.
    """

    if weights is None:
        weights = [0.6, 0.4]
    if len(doc_lists) != len(weights):
        raise ValueError(
            "Number of rank lists must be equal to the number of weights."
        )

    # Create a union of all unique documents in the input doc_lists
    all_documents = set()
    for doc_list in doc_lists:
        for doc in doc_list:
            all_documents.add(doc.get('_id'))

    # Initialize the RRF score dictionary for each document
    rrf_score_dic = {doc: 0.0 for doc in all_documents}

    # Calculate RRF scores for each document
    for doc_list, weight in zip(doc_lists, weights):
        for rank, doc in enumerate(doc_list, start=1):
            rrf_score = weight * (1 / (rank + c))
            rrf_score_dic[doc.get('_id')] += rrf_score

    # Sort documents by their RRF scores in descending order
    sorted_documents = sorted(
        rrf_score_dic.keys(), key=lambda x: rrf_score_dic[x], reverse=True
    )

    # Map the sorted _id back to the original document
    id_to_doc_map = {
        doc.get('_id'): doc for doc_list in doc_lists for doc in doc_list
    }
    sorted_docs = [
        id_to_doc_map[_id] for _id in sorted_documents
    ]

    return sorted_docs


def filter_hybrid_searched_files(files):
    """
    filter duplicate files
    """

    path_set = set()
    filtered_files = []
    for file in files:
        fullpath = file.get('fullpath')
        if fullpath in path_set:
            continue
        path_set.add(fullpath)
        file.pop('_id', None)
        file.pop('score', None)
        file.pop('max_score', None)
        filtered_files.append(file)
    return filtered_files


def bulk_add_sentences_to_index(seasearch_api, embedding_api, index_name, path, sentences, limit=1000):
    step = limit
    start = 0
    while True:
        if not sentences[start: start + step]:
            break
        params = get_document_add_params(embedding_api, sentences[start: start + step], index_name, path)
        seasearch_api.bulk(index_name, params)
        start += step
