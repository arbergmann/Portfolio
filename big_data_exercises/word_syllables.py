from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools
import syllapy
import re
# WORD_RE = re.compile(r"[\w']+") # any whitespace or apostrophe, used to split lines below
WORDS_ONLY_RE = re.compile(r"[a-zA-Z]+") # any whitespace or apostrophe, used to split lines below
class MRWordSyllables(MRJob):
    
    STOPWORDS = {'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and',
                 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below',
                 'between', 'both', 'but', 'by', 'can', 'did', 'do', 'does', 'doing', 'don',
                 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'has', 'have',
                 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his',
                 'how', 'i', 'if', 'in', 'into', 'is', 'it', 'its', 'itself', 'just', 'me',
                 'more','most', 'my', 'myself', 'no', 'nor', 'not', 'now', 'of', 'off', 'on',
                 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own',
                 's', 'same', 'she', 'should', 'so', 'some', 'such', 't', 'th', 'than', 'that', 'the',
                 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they',
                 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was',
                 'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why',
                 'will', 'with', 'you', 'your', 'yours', 'yourself', 'yourselves'}
    
    word_seen = set()
    
    def steps(self):
        
        return [MRStep(mapper = self.mapper_get_words,
                       combiner = self.combiner_swap_tuple,
                       reducer = self.reducer_order_set)]
    
    def mapper_get_words(self, _, line):
        # lower and extract words from each line of data
        for word in WORDS_ONLY_RE.findall(line.lower()):
            # if the word is not a stopword and has not already been processed
            # update word_seen and yield word and syllable count
            if word not in self.STOPWORDS and word not in self.word_seen:
                self.word_seen.add(word)
                yield (word, syllapy.count(word))
                
    def combiner_swap_tuple(self, word, syllable_count):
        # effectively zip syllable_count and word into a single 2-tuple
        yield (None, (word, syllable_count))
                
    def reducer_order_set(self, _, syllable_count_pairs):
        # sort the tuples by syllable count and word, storing the top 10
        sorted_list = sorted(syllable_count_pairs,
                             key=lambda pairs: (pairs[1], pairs[0]))[-1:-11:-1]
        
        # iterate the sorted list extracting each list element
        # yielding each item from the element
        for (word, syllables) in sorted_list:
            yield (word, syllables[0])
            
if __name__ == '__main__':
    import time
    start = time.time()
    MRWordSyllables.run()
    end = time.time()
    print(end - start)
