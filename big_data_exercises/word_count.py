from mrjob.job import MRJob
import re

class MRWordFrequencyCount(MRJob):

  ### input: self, in_key, in_value
  def mapper(self, _, line):
    yield "chars", len(line)           # how many characters in a line, regardless of alphanumeric
    yield "words", len(line.split())   # split line into words based on spaces, to be mapped, return length of list
    yield "lines", 1                   # returns number of lines analyzed on pass, each line one pass (value of 1)

  ### input: self, in_key from mapper, in_value from mapper
  def reducer(self, key, values):
    yield key, sum(values)             # for each key ("chars", "words", "lines") return sum of values for each key

if __name__ == "__main__":
    MRWordFrequencyCount.run()
