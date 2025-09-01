import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class Identifier:

    def __init__(self):
        nltk.data.find("sentiment/vader_lexicon.zip")

    @staticmethod
    def sentiment_of_text(text):
        score = SentimentIntensityAnalyzer().polarity_scores(text)
        if score['compound'] >= 0.5:
            return 'positive'
        elif score['compound'] >= -0.49:
            return "neutral"
        else:
            return "negative"

    @staticmethod
    def weapon_in_text(text, weapons: list):
        words = text.split()
        detected = [w for w in weapons if w in words]
        return " ".join(detected)