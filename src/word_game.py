
import requests
from bs4 import BeautifulSoup
import re


def obfuscate(text, word):
    return re.sub('\w*'+ word + '\w*', 'xxx', text)


def lookup(word):
    response = requests.get(f"https://ordnet.dk/ddo/ordbog?query={word}")
    if response.status_code != 200:
        raise Exception(response.text)

    soup = BeautifulSoup(response.text, features="html.parser")
    definitions = soup.findAll("span", {"class": "definition"})

    synonymsElement = soup.findAll("div", {"class": "onym"})[0]
    synonyms = synonymsElement.findAll("span", {"class": "inlineList"})[0].text

    relatedWordsElement = soup.findAll("div", {"class": "rel-begreber"})[0]
    relatedWords = relatedWordsElement.findAll("span", {"class": "inlineList"})[0].text

    citations = soup.findAll("span", {"class": "citat"})

    return {
        "citations": [obfuscate(x.text, word) for x in citations],
        "synonyms": synonyms,
        "related": relatedWords
    }


def generate_citations(word):
    return [obfuscate(x, word) for x in lookup(word)]


words = [
    # "årsskifte",
    "yak"
]

for word in words:
    res = lookup(word)
    print("guess this word:")
    for citation in res['citations']:
        print(citation)
    print("synonyms: " + res['synonyms'])
    print("related: " + res['related'])
    print()


