def new_list():
    newlist={
        'elements':[],
        'size':0,
    }
    return newlist

def get_element(my_list, index):
    return my_list["elements"][index]

def is_present(my_list,element, cmp_function):
    size=my_list["size"]
    if size>0:
        keyexist=False
        for keypos in range(0,size):
            info=my_list["elements"][keypos]
            if cmp_function(element,info)==0:
                keyexist=True
                break
            if keyexist:
                return keypos
    return -1

def add_first(my_list,element):
     my_list["elements"].insert(0, element)  
     my_list["size"] += 1
     return my_list

def add_last(my_list,element):
    my_list["elements"].append(element)
    my_list["size"] += 1    
    return my_list

def size(my_list):
    return my_list["size"]

def first_element(my_list):
    return my_list["elements"][0]

def is_empty(my_list):
    empty=False
    if my_list["size"]==0:
        empty=True
    return empty
        
def last_element(my_list):
    return my_list["elements"][size(my_list)-1]

def delete_element(my_list, pos):
    my_list["elements"].pop(pos)
    my_list["size"]-=1
    return my_list
    
    
def remove_first(my_list):
    val=my_list["elements"].pop(0)
    my_list["size"]-=1
    return val

def remove_last(my_list):
    val=my_list["elements"].pop()
    my_list["size"]-=1
    return val
 
def insert_element(my_list, element, pos):
    my_list["elements"].insert(element,pos)
    my_list["size"]+=1
    return my_list

def change_info(my_list, pos, new_info):
    my_list["elements"][pos]=new_info
    return my_list

def exchange(my_list, pos1, pos2):
    x=my_list["elements"][pos1]
    my_list["elements"][pos1]=my_list["elements"][pos2]
    my_list["elements"][pos2]=x
    return my_list

def sub_list(my_list, pos_i, num_elements):
    sub=new_list()
    sub["size"]=num_elements
    sub["elements"]=my_list["elements"][pos_i:pos_i+num_elements]
    return sub