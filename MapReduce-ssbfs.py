from mrjob.job import MRJob
import re
import mrjob.protocol

class SSBFS(MRJob):

	INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
  
	def CharMapper(self, char1, values):
		vertex = char1 # current character node
		distance = list(values)[0] # distance from source node         
		AdjList = list(values)[1] # list of related characters
		yield vertex, ('nodes', AdjList) # pass back adjacency list

        # if the distance is already initialized to an integer,
        # pass all adjacent nodes the current distance + 1
		if isinstance(distance, int): 
            # pass current distance from node back to current vertex before adding 1
			yield vertex, ('distance', distance) 
			distance += 1 # then add 1 to pass to adjacent nodes
		for char in AdjList:
			yield char, ('distance', distance) # pass new distance to adjacent nodes
    
	def CharReducer(self, node, values):
		distList = list()
		for value_type, value in values: # list all  distances passed from adjacent nodes
			if value_type == 'distance':
				distList.append(value)
			else: AdjList = list(value) # otherwise it's the adjacency list itself
		if not any(distList): distance = None # if the list is all null, new distance still null
		else: distance = min(d for d in distList if d is not None) # or take minimum distance
		yield node, [distance, AdjList]

	def steps(self):
		return [self.mr(mapper= self.CharMapper, reducer = self.CharReducer)]*4

if __name__ == '__main__':
	SSBFS.run()
