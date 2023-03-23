import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords


class NLP:
    def __init__(self):
        self.stopwords = set(stopwords.words('english'))
    
    def invertedIndex(self, txt_lst):
        ii = {}
        txt_idx = 0
        for item in txt_lst:
            word = [self.tokenize(word) for word in item.split() if word not in self.stopwords]
            ii = self.add2dict(nltk.FreqDist(word), ii, txt_idx)
            txt_idx += 1
        del ii['']
        return ii
            
    def add2dict(self, freq, ii, idx):
        for word in freq.keys():
            if word in ii.keys():
                ii[word].append((idx, freq[word]))
            else:
                ii[word] = [(idx, freq[word])]
        
        return ii

    def tokenize(self, word):
        'remove punctuation from word string'
        word = ''.join([letter.lower() if letter.isalpha() else '' for letter in word])
        if len(word) > 0:
            return nltk.PorterStemmer().stem(word)
        return ''