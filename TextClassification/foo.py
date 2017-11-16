import csv
data = []
with open('data.csv') as csvfile:
    reader = csv.DictReader(csvfile)    
    for row in reader:
        text = " ".join(row.values())
        data.append(text)

import nltk
from nltk.tag.stanford import StanfordPOSTagger

spanish_postagger = StanfordPOSTagger('models/spanish.tagger', './stanford-postagger.jar')


data = data[:50]

punctuations = ['•','/']

translator = str.maketrans("".join(punctuations),' '*len(punctuations))
proc_data = []
for text in data:
    text = text.lower()
    text = text.translate(translator)
    proc_data.append(text)
    
data = proc_data

from nltk.tokenize import word_tokenize

terms_by_tag = {}
terms_by_tag['n'] = set()
terms_by_tag['v'] = set()
terms_by_tag['a'] = set()
terms_by_tag['c'] = set()
terms_by_tag['d'] = set()
terms_by_tag['f'] = set()
terms_by_tag['i'] = set()
terms_by_tag['p'] = set()
terms_by_tag['r'] = set()
terms_by_tag['s'] = set()
terms_by_tag['w'] = set()
terms_by_tag['z'] = set()


for text in data:
    tag_terms = spanish_postagger.tag(word_tokenize(text))
    for term in tag_terms:
        if term[0] == 'kardex':
            print(term[1])
        for tag in terms_by_tag:
            if term[1][0] == tag:
                terms_by_tag[tag].add(term[0]) 


vocab = set()
vocab.update(terms_by_tag['n'])
vocab.update(terms_by_tag['v'])
vocab.update(terms_by_tag['a'])

# n1 + s + n2
# base de datos
for n1 in terms_by_tag['n']:
    for s in terms_by_tag['s']:
        for n2 in terms_by_tag['n']:
            vocab.add(" ".join([n1, s, n2]))

print(len(vocab))
