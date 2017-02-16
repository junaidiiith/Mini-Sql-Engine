from __future__ import print_function
import csv
import sys
import re
from collections import OrderedDict


def readFile(tName,fileData):
	with open(tName,'rb') as f:
		reader = csv.reader(f)
		for row in reader:
			fileData.append(row)

def rem_spaces(q):
	return (re.sub(' +',' ',q)).strip()

def main():
	Tables_map = {}
	create_table_signatures("metadata.txt",Tables_map)
	query = str(sys.argv[1])
	parse_query(query,Tables_map)

def create_table_signatures(filename,dictionary):
	with open(filename,'rb') as f:
		flag = 0
		for line in f:
			if line.strip() == "<begin_table>":
				flag = 1
				continue
			if flag == 1:
				tableName = line.strip()
				dictionary[tableName] = [];
				flag = 0
				continue
			if not line.strip() == '<end_table>':
				dictionary[tableName].append(tableName+"."+line.strip())

def check_tables(tableNames,dictionary):
	for t in tableNames:
		try:
			d = dictionary[t]
		except:
			sys.exit("Table not found error")



def check_attributes(attributes,tablenames,dictionary):
	try:
		if attributes[0] == "*":
			attributes = dictionary[tablenames[0]]
		else:
			for a in attributes:
				present = False
				for key,value in dictionary.items():
					if a in value:
						if present:
							sys.exit("Ambigious case for attibute "+a)
						present = True
						break
				if not present:
					sys.exit("Attribute "+ a + " not present")
	except:
		sys.exit("Attribute not found")
	return attributes

def get_join_conditions(dictionary,join_conditions):
	Jc = {}
	if join_conditions:
		for j in join_conditions:
			c = parse_condition(rem_spaces(j))
			Jc[(c[0],c[1])] = (c[2],c[3])
			Jc[(c[1],c[0])] = (c[3],c[2])
	return Jc


def add_tableName(attributes,tableNames,dictionary):
	for a in attributes:
		if "." not in a:
			found = 0
			for key in tableNames:
				for v in dictionary[key]:
					if a == v.split('.')[1]:
						attributes[attributes.index(a)] = v
						found = 1
						break
				if found:
					break
	return attributes

def parse_query(query,dictionary):
	query = rem_spaces(query)

	# print(query)
	keywords = ["select","from","where","distinct","sum","avg","min","max"]
	for k in keywords:
		if k in query.lower():
			l1 = len(query.lower().split(k)[0])
			l2 = len(query.lower().split(k)[1])
			query = query[0:l1] + k + query[l1+len(k):len(query)]
			# print(query)

	d = tuple()
	# print(query)
	if "from" in query:
		from_split = query.split('from')
	else:
		sys.exit("No from clause")

	select_and_attributes_part = from_split[0].strip()

	if "select" in select_and_attributes_part:
		select_split = select_and_attributes_part.strip().split('select')[1]
	else:
		sys.exit("No select attributes present")


	select_split = rem_spaces(select_split)
	if "distinct" in select_split:
		attributes = rem_spaces(select_split.strip().split('distinct')[1])
		distinct = True
		if len(rem_spaces(select_split.strip().split('distinct')[0])):
			sys.exit("Incorrect query format")
	else:
		attributes = select_split
		distinct = False
	attributes = rem_spaces(attributes)
	attributes = [rem_spaces(x) for x in attributes.split(',')]

	where_split = rem_spaces(from_split[1]).split('where')
	tableNames = [rem_spaces(x) for x in rem_spaces(where_split[0]).split(',')]
	check_tables(tableNames,dictionary)

	if distinct:
		for a in attributes:
			if '(' in a or ')' in a:
				d_att = a.split('(')[1][:-1]
				attributes[attributes.index(a)] = d_att
				break
		aggr_func = ""
		x = add_tableName([d_att],tableNames,dictionary)
		d = (x[0],True)

	temp = []
	aggr_func = ""
	for a in attributes:
		if '(' in a or ')' in a:
			aggr_func = a
		else:
			temp.append(a)
	attributes = temp


	if aggr_func:
		if '.' not in aggr_func:
			attributes = [rem_spaces(aggr_func).split('(')[1][:-1]]
			attributes = add_tableName(attributes,tableNames,dictionary)
			aggr_func = rem_spaces(aggr_func).split('(')[0] + '(' + attributes[0] + ')'

	attributes = add_tableName(attributes,tableNames,dictionary)
	attributes = check_attributes(attributes,tableNames,dictionary)
	
	if len(where_split) > 1:
		conditions = rem_spaces(where_split[1])
	else:
		conditions = ""

	run(dictionary,tableNames,attributes,conditions,d,aggr_func)

def parse_condition(condition):
	x = condition.split("=")
	L = []
	L.append(rem_spaces(x[0]).split('.')[0])
	L.append(rem_spaces(x[1]).split('.')[0])
	L.append(rem_spaces(x[0]).split('.')[1])
	L.append(rem_spaces(x[1]).split('.')[1])
	return tuple(L)
	
def print_result(resultant_data,attributes,schema,d,aggr_func):

	if len(d):
		i = 1
		for a in attributes:
			if i == 1:
				print(a,end="")
				i = 0
			else:
				print(","+a,end="")
		print("\n")

		h = {}
		for idx,row in enumerate(resultant_data):
			try:
				if h[row[schema.index(d[0])]]:
					continue
			except:
				h[row[schema.index(d[0])]] = idx

		for key,value in h.items():
			i = 1
			for col in attributes:
				data = resultant_data[value]
				if i == 1:
					i = 0
					print(data[schema.index(col)],end="")
				else:
					print(","+data[schema.index(col)],end="")
			print("\n")

	elif aggr_func:
		type_of_func = rem_spaces(rem_spaces(aggr_func).split('(')[0]).lower()
		print_aggr(type_of_func,resultant_data,schema,rem_spaces(rem_spaces(aggr_func).split('(')[1][:-1]))

	else:
		i = 1
		for a in attributes:
			if i == 1:
				print(a,end="")
				i = 0
			else:
				print(","+a,end="")
		print("\n")
		
		for data in resultant_data:
			i = 1
			for col in attributes:
				if i == 1:
					print(data[schema.index(col)],end="")
					i = 0
				else:
					print(","+data[schema.index(col)],end="")
			print("\n")


def print_aggr(tof,res,schema,agg_att):
	
	l = []
	for r in res:
		l.append(int(r[schema.index(agg_att)]))
	if tof == "sum":
		print("Sum of "+agg_att)
		print(sum(l))
	elif tof == "avg":
		print("Avg of "+agg_att)
		print(sum(l)/len(l))
	elif tof == "max":
		print("max of "+agg_att)
		print(max(l))
	elif tof == "min":
		print("min of "+ agg_att)
		print(min(l))
	else:
		sys.exit("Unknown aggregate function")




def run(dictionary,tableNames,attributes,conditions,d,aggr_func):
	conditions = re.sub(' ?= ?','=',conditions)
	conditions = [rem_spaces(c) for c in conditions.split(" ")]
	
	if len(conditions[0]):
		resultant_data = []
		join_conditions = []
		const_conditions = ""

		for c in conditions:
			x = c.split("=")
			try:
				RHS = rem_spaces(x[1])
			except:
				RHS=""
			if '.' not in RHS:
				const_conditions += " " + c + " "


		const_conditions = rem_spaces(const_conditions)

		for c in conditions:
			x = c.split('=')
			try:
				RHS = rem_spaces(x[1])
			except:
				RHS =""
			if '.' in RHS:
				join_conditions.append(c)
		resultant_data,schema = join(dictionary,tableNames,const_conditions,join_conditions)
		print_result(resultant_data,attributes,schema,d,aggr_func)
	
	elif len(tableNames) > 1:
		resultant_data,schema = join(dictionary,tableNames,None,None)
		print_result(resultant_data,attributes,schema,d,aggr_func)

	else:
		resultant_data = []
		schema = dictionary[tableNames[0]]
		readFile(tableNames[0]+".csv", resultant_data)
		print_result(resultant_data,attributes,schema,d,aggr_func)

			


def rem_via_constants(resultant_data,const_conditions,schema,dictionary,tableNames):
	new = []
	for data in resultant_data:
		s = evaluate(data,const_conditions,dictionary,schema,tableNames)
		# print(s)
		if len(s):
			if eval(s):
				new.append(data)
	resultant_data = new
	return (resultant_data,schema)


def evaluate(data,const_conditions,dictionary,schema,tableNames):

	const_conditions = rem_spaces(re.sub('=',' = ',const_conditions)).split(" ")
	string = ""
	relational = ['and','or']
	if const_conditions[0].lower() in relational:
		const_conditions.pop(0)
	lhs = True
	for i in const_conditions:
		if i == "=":
			string += i*2
		elif i.lower() == 'and' or i.lower() == 'or':
			string += ' ' + i.lower() + ' '
		elif i and lhs:
			lhs = False
			if i.split('.')[0] not in dictionary.keys() and ('.' in i):
				sys.exit("No table found by name" + i.split('.')[0])
			else:
				try:
					i = add_tableName([i],tableNames,dictionary)
					string += data[schema.index(i[0])]
				except:
					string=""
		else:
			lhs = True
			string += i
	return string


def join_tables(resultant_data,table_data,table1,table2,schema,r_att,t_att,dictionary):

	if r_att and t_att:
		h = {}
		new = []
		i = schema.index(table1+"."+r_att)
	
		for idx,row in enumerate(resultant_data):
			h[row[i]] = idx
		
		i = dictionary[table2].index(table2+"."+t_att)

		for row in table_data:
			if h.has_key(row[i]):
				new.append(resultant_data[h[row[i]]] + row)
		resultant_data = new
		
	else:
		new = []
		for r in resultant_data:
			for t in table_data:
				new.append(r+t)
		resultant_data = new
	schema += dictionary[table2]

	return (resultant_data,schema)
		
	
	

def join(dictionary,tableNames,const_conditions,join_conditions):


	database = {}
	visited = {}
	for t in tableNames:
		visited[t] = False
		database[t] = []
		readFile(t+".csv", database[t])

	Jc = get_join_conditions(dictionary,join_conditions)
	remove_attribs = []
	i = 1
	for t in tableNames:
		if i == 1:
			resultant_data = database[t]
			visited[t] = True
			schema = dictionary[t]
			i = 0
		else:
			for key,value in visited.items():
				if visited[key]:
					try:
						join_attribs = Jc[(t,key)]
					except:
						join_attribs = None
					if join_attribs:
						remove_attribs.append(t+'.'+join_attribs[0])
						resultant_data,schema = join_tables(resultant_data,database[t],key,t,schema,join_attribs[1],join_attribs[0],dictionary)
					else:
						resultant_data,schema = join_tables(resultant_data,database[t],key,t,schema,None,None,dictionary)

	if const_conditions:
		if "=" in const_conditions:
			if len(const_conditions):
				resultant_data,schema = rem_via_constants(resultant_data,const_conditions,schema,dictionary,tableNames)

	for r in remove_attribs:
		try:
			schema.remove(r)
		except:
			print("No such attribute present")

	return resultant_data,schema



if __name__ == "__main__":
	main()