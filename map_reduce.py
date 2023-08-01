from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

# I'm not sure how to output in ABC order other than
# python map_reduce.py input1.txt | sort  > output1.txt

class MRUniqueWords(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word, 1

    def reducer(self, word, counts):
        yield word, 1


if __name__ == '__main__':
    MRUniqueWords.run()
