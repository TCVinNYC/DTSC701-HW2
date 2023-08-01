from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRBigramCount(MRJob):
    # python map_bigramcount.py input3.txt > output3.txt

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for i in range(len(words) - 1):
            yield (words[i].lower() + "," + words[i+1].lower()), 1

    def reducer(self, bigram, counts):
        yield bigram, sum(counts)


if __name__ == '__main__':
    MRBigramCount.run()
