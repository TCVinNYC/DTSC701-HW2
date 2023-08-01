from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRInvertedIndex(MRJob):

    def mapper(self, _, line):
        # The format of each line is "DocumentID: text"
        doc_id, text = line.split(": ", 1)
        for word in WORD_RE.findall(text):
            yield word.lower(), doc_id

    def reducer(self, word, doc_ids):
        # We use a set to remove any duplicate doc IDs
        unique_doc_ids = list(set(doc_ids))
        yield word, ", ".join(sorted(unique_doc_ids))


if __name__ == '__main__':
    MRInvertedIndex.run()
