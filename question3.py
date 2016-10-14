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

#Tokenize the sentence to words and filter the sentence with "empty" sentiment and some short words (it is not useful)
tweets = []
for index, row in df[['content', 'sentiment']].iterrows():
    line = str(row['content']).decode('utf-8','ignore')     #When tokenize the sentence, need to use .decode('utf-8', 'ignore') for avoiding from error. Further information: http://nedbatchelder.com/text/unipain.html
    for e in nltk.word_tokenize(line):
        if len(e) >= 3 and str(row['sentiment']) != 'empty':
            tweets.append((e.lower(), str(row['sentiment'])))
            
#Shuffling the data to let them be random
shuffle(tweets) 

#Using nltk stopwords function to filter some common useless words
newTweets = []
for (word, sentiment) in tweets: 
    if word not in stopwords.words('english'):
        newTweets.append(({'feature':word}, sentiment)) 

#Segregating data to 3/4 for training and 1/4 for testing
poscutoff = len(newTweets)*3/4 
trainfeats = newTweets[:poscutoff][:]
testfeats = newTweets[poscutoff:][:]

#Getting out the feature of testing data for preticting test later
testdata = []
for (dist, sentiment) in testfeats:
    testdata.append(dist)

#Naive Bayes Classifier accuracy: 0.317835365854(approximately) with testing new data
#accuracy: 0.559451219512 with testing part of the training data
classifier = nltk.NaiveBayesClassifier.train(trainfeats)
predict = classifier.classify_many(testdata)
classifier.show_most_informative_features()
#print(predict)

#Decision Tree accuracy: 0.178353658537(approximately) with testing new data
#accuracy: 0.640243902439 with testing part of the training data
classifier2 = nltk.classify.DecisionTreeClassifier.train(trainfeats, entropy_cutoff=0, support_cutoff=0)
predict2 = classifier2.classify_many(testdata)
#print(predict2)

#BernoulliNB accuracy: 0.321646341463(approximately) with testing new data
#accuracy: 0.475609756098 with testing part of the training data
classif = SklearnClassifier(BernoulliNB()).train(trainfeats)
predict3 = classif.classify_many(testdata)
#print(predict3)

#SVM accuracy: 0.326219512195(approximately) with testing new data
#accuracy: 0.326219512195 with testing part of the training data
classif2 = SklearnClassifier(SVC(), sparse=False).train(trainfeats)
predict4 = classif2.classify_many(testdata)
#print(predict4)

#Printing the result of model accuracy by using testing data
print 'accuracy:', nltk.classify.util.accuracy(classifier, testfeats)
print 'accuracy:', nltk.classify.util.accuracy(classifier2, testfeats)
print 'accuracy:', nltk.classify.util.accuracy(classif, testfeats)
print 'accuracy:', nltk.classify.util.accuracy(classif2, testfeats)


"""
>>> df.info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 40000 entries, 0 to 39999
Data columns (total 4 columns):
tweet_id     40000 non-null int64
sentiment    40000 non-null object
author       40000 non-null object
content      40000 non-null object
dtypes: int64(1), object(3)
memory usage: 1.2+ MB

>>> classifier.labels()
['love',
 'relief',
 'neutral',
 'anger',
 'sadness',
 'happiness',
 'surprise',
 'fun',
 'enthusiasm',
 'hate',
 'worry',
 'boredom']
 
 #After running the above code, result will be like this:
 
 Most Informative Features
                 feature = u'love'          love : anger  =    163.2 : 1.0
                 feature = u'happy'         love : boredo =    115.4 : 1.0
                 feature = u'hate'          hate : boredo =    103.7 : 1.0
                 feature = u'sad'         sadnes : fun    =     92.3 : 1.0
                 feature = u'thanks'      happin : boredo =     79.2 : 1.0
                 feature = u'mothers'       love : hate   =     78.4 : 1.0
                 feature = u'http'        neutra : boredo =     72.2 : 1.0
                 feature = u'day'           love : anger  =     66.9 : 1.0
                 feature = u'good'        happin : boredo =     64.5 : 1.0
                 feature = u'fun'            fun : anger  =     61.6 : 1.0
accuracy: 0.32713963964
accuracy: 0.282094594595
accuracy: 0.326576576577
accuracy: 0.311373873874
"""
