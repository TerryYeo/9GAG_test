"""Running python in Anaconda environment and coding on Jupyter Notebook. 
Using NLTK library and Scikit-learn for Nature Language Processing and Machine Learning.
Using pandas for dataframe data structure.
I have built four model including Naive Bayes, Decision Tree, BernoulliNB and SVM."""

from nltk.classify import SklearnClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.svm import SVC
import nltk
from nltk.corpus import stopwords
from nltk.classify import NaiveBayesClassifier
import numpy as np
import pandas as pd

#Reading data and create a dataframe
df = pd.read_csv('/home/terry/Downloads/text_emotion.csv')

#Tokenize the sentence to words and 
tweets = []
for index, row in df[['content', 'sentiment']].iterrows():
    line = str(row['content']).decode('utf-8','ignore')
    for e in nltk.word_tokenize(line):
        if len(e) >= 3 and str(row['sentiment']) != 'empty':
            tweets.append((e.lower(), str(row['sentiment'])))
shuffle(tweets)

newTweets = []
for (word, sentiment) in tweets: 
    if word not in stopwords.words('english'):
        newTweets.append(({'feature':word}, sentiment)) 
        
poscutoff = len(newTweets)*3/4
 
trainfeats = newTweets[:poscutoff][:]
testfeats = newTweets[poscutoff:][:]

testdata = []
for (dist, sentiment) in testfeats:
    testdata.append(dist)

#Naive Bayes Classifier accuracy: 0.317835365854(approximately) with testing new data
#accuracy: 0.559451219512 with testing part of the training data
classifier = nltk.NaiveBayesClassifier.train(newTweets)#trainfeats)
predict = classifier.classify_many(testdata)
classifier.show_most_informative_features()
#print(predict)

#Decision Tree accuracy: 0.178353658537(approximately) with testing new data
#accuracy: 0.640243902439 with testing part of the training data
classifier2 = nltk.classify.DecisionTreeClassifier.train(newTweets, entropy_cutoff=0, support_cutoff=0)
predict2 = classifier2.classify_many(testdata)
#print(predict2)

#BernoulliNB accuracy: 0.321646341463(approximately) with testing new data
#accuracy: 0.475609756098 with testing part of the training data
classif = SklearnClassifier(BernoulliNB()).train(newTweets)#trainfeats)
predict3 = classif.classify_many(testdata)
#print(predict3)

#SVM accuracy: 0.326219512195(approximately) with testing new data
#accuracy: 0.326219512195 with testing part of the training data
classif2 = SklearnClassifier(SVC(), sparse=False).train(newTweets)#trainfeats)
predict4 = classif2.classify_many(testdata)
#print(predict4)

print 'accuracy:', nltk.classify.util.accuracy(classifier, testfeats)
print 'accuracy:', nltk.classify.util.accuracy(classifier2, testfeats)
print 'accuracy:', nltk.classify.util.accuracy(classif, testfeats)
print 'accuracy:', nltk.classify.util.accuracy(classif2, testfeats)
