from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRNonStopWords(MRJob):
    # python map_stopwords.py input2.txt > output2.txt

    stop_words = set(['the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'])

    def mapper_get_words(self, _, line):
        # Yield each non-stop word in the line.
        for word in WORD_RE.findall(line):
            if word.lower() not in self.stop_words:
                yield (word.lower(), 1)

    def reducer_count_words(self, word, counts):
        # Yield each word and its count.
        yield word, sum(counts)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words)
        ]


if __name__ == '__main__':
    MRNonStopWords.run()
