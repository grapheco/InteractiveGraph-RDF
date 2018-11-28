from bs4 import BeautifulSoup
import re

filename = "./honglou.xml"

xml = open(filename, 'r', encoding='utf-8')
soup = BeautifulSoup(xml, 'lxml')

nodes = soup.find_all('nodes')
edges = soup.find_all('edges')

n3name = './honglou.n3'
with open(n3name, "w+", encoding='utf-8') as n3:
    n3.write("@prefix honglou: <http://interactivegraph.org/app/honglou/>." + '\n')
    n3.write("@prefix node: <http://interactivegraph.org/data/node/>."+'\n')
    n3.write("@prefix edge: <http://interactivegraph.org/data/edge/>."+'\n')
    for node in nodes:

        id = node.find('id').string
        subject_str = "node:"+id
        id_str = '\thonglou:id '+"\""+id+"\""

        label=node.find('label').string
        label_str = '\thonglou:label '+"\""+label+"\""

        value = node.find('value').string
        value_str = '\thonglou:value '+"\""+value+"\""

        categories = node.find('categories').string
        categories_str = '\thonglou:categories '+"\""+categories+"\""

        info = node.find('info').string
        info_str = '\thonglou:info'+"\""+info+"\""

        n3.write(subject_str + '\n')
        n3.write(id_str+';\n')
        n3.write(label_str+';\n')
        n3.write(value_str+';\n')
        n3.write(categories_str+';\n')
        n3.write(info_str+'.\n')

    for edge in edges:

        id = edge.find('id').string
        subject_str = "edge:"+id
        id_str = '\thonglou:id '+"\""+id+"\""

        label = edge.find('label').string
        label_str = '\thonglou:label ' + "\"" + label + "\""

        fromId = edge.find('from').string
        fromId_str = '\thonglou:from ' + "\"" + fromId + "\""

        toId = edge.find('to').string
        toId_str = '\thonglou:to ' + "\"" + toId + "\""

        n3.write(subject_str + '\n')
        n3.write(id_str + ';\n')
        n3.write(label_str + ';\n')
        n3.write(fromId_str + ';\n')
        n3.write(toId_str + '.\n')