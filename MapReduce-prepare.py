from mrjob.job import MRJob
import re

class AdjacencyList(MRJob):
  
    def SwitchCharCom(self, _, line):
        # Find the names between quotation marks 
        words = re.search('\"(.*)\"\s*\"(.*)\"',line)                                          
        Character = words.group(1) # The first string is the character
        Comic = words.group(2) # The second string is the comic
        yield Comic, Character
    
    def CharList(self, comic, character):
        AllCharacters = list(character) # need to change generator into list
        yield comic, AllCharacters # return comic and list of characters in that comic
    
    def CharPairs(self, comic, AllCharacters):
        for char1 in AllCharacters:
            for char2 in AllCharacters:
                 if char1 != char2: yield char1, char2
    
    def CharAssociation(self, char1, char2):
        char2Long = list(char2) # switch to a list of characters
        char2unique = list(set(char2Long)) # get unique list of characters
        yield char1, (None, char2unique)
        
    def steps(self):
        return ([self.mr(mapper = self.SwitchCharCom, reducer = self.CharList),
        self.mr(mapper = self.CharPairs, reducer = self.CharAssociation)])

if __name__ == '__main__':
    AdjacencyList.run()