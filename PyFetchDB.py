import ast
from itertools import count
import os


# ---VERSION 2.4.5 ---

class database:
    def __init__(self, Apath):  # setting object path to path
        self.path = Apath
        self.line_count = 0
        self.update_fcache = None  # used by the update() func to go faster
        if self.is_valid_path():
            p = Apath.split('.')
            if p[len(p)-1] != 'pf':
                self.ValueError_func(f"File has to have PyFetch extension (pf), not ({p[1]})")
        else:
            self.FileNotFoundError_func(f"Path is not valid: {self.path}")

    ##### ERRORS #####

    def FileNotFoundError_func(self, details):
        raise FileNotFoundError(f"\n\n{details}\n")

    def generic_exception(self, details):
        raise Exception(f"\n\n{details}\n")

    def TypeError_func(self, details):
        raise TypeError(f"\n\n{details}\n")

    def IndexError_func(self, details):
        raise IndexError(f"\n\n{details}\n")

    def ValueError_func(self, details):
        raise ValueError(f"\n\n{details}\n")

    ##### ERRORS #####

    def count_lines(self):
        with open(self.path, 'r') as file:
            return file.read().count('\n')

    def line_num_through_id(self, id):
        with open(self.path, 'r') as file:
            lines = file.readlines()
        for count, line in enumerate(lines):
            if f"{id}---" in line:
                return count
        self.IndexError_func(f"Id ({id}) not    found.")

    def is_valid_path(self):  # there must always be a dot for the extension, eg: .txt, .exe, .dll, .mp3
        if '.' not in self.path:
            return False
        else:
            return True

    def make(self):  # creates db if not exists
        if self.is_valid_path():  # check if path is valid
            if not os.path.exists(self.path):  # check if file exists, and if it doesn't:
                open(self.path, 'a').close()  # open then close right after to make empty file
                self.line_count = 0
            else:
                self.line_count = self.count_lines()
        else:
            self.FileNotFoundError_func(f"Path is not valid: {self.path}")

    def insert(self, dic):  # inserts to db (has to be a dictionary)
        if str(type(dic)) == "<class 'dict'>":  # ensures input is a dictionary
            try:
                if "---" not in str(dic):
                    line_num = self.line_count
                    with open(self.path, 'a') as file:
                        file.write(f"{line_num}---{dic}\n")
                    self.line_count += 1
                else:
                    self.ValueError_func("'---' is an illegal set of characters.")
            except FileNotFoundError:
                self.FileNotFoundError_func(
                    f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")
        else:
            self.TypeError_func("Input has to be a dictionary.")

    def query(self, id):  # query's database and returns line corresponding to the line id
        if str(type(id)) == "<class 'int'>":  # making sure id is an integer
            try:
                s_id = str(id)
                with open(self.path, 'r') as file:
                    lines = file.readlines()
                for line in lines:
                    if f"{s_id}---" in line:
                        return ast.literal_eval(line.split('---')[1])
                self.IndexError_func(f"id ({s_id}) not found")
            except FileNotFoundError:
                self.FileNotFoundError_func(
                    f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")
        else:
            self.TypeError_func(f"Id has to be an integer, not {str(type(id))}")

    def remove(self, id):  # removes line corresponding to the id
        if str(type(id)) == "<class 'int'>":  # making sure id is an integer
            try:
                with open(self.path, 'r') as file:
                    contents = file.readlines()
                for count, line in enumerate(contents):
                    if f"{id}---" in line:
                        del contents[count]
                        with open(self.path, 'w') as file:
                            file.writelines(contents)  # rewriting lines to file
                            return
                self.IndexError_func(f"Id ({id}) not found.")
            except FileNotFoundError:
                self.FileNotFoundError_func(
                    f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def fetch_all(self):  # returns all dictionaries in a list
        try:
            with open(self.path, 'r') as file:
                return [ast.literal_eval(line.split('---')[1]) for line in file.readlines()]  # return all dics line by line
        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def fetch_all_raw(self):
        try:
            with open(self.path, 'r') as file:
                return file.readlines()
        except FileNotFoundError:
            self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def update(self, id, dic, fast=False, file_obj=None):
        try:
            id_search = f"{id}---"
            #breakpoint()
            if file_obj:  # user opened file for us
                raw = file_obj.readlines()
                # setting up fcache
                if not self.update_fcache:
                    self.update_fcache = raw
                for count, line in enumerate(raw):
                    if id_search in line:
                        self.update_fcache[count] = f"{id}---{dic}\n"
                        break
                print(F"\n\n\n\n{self.update_fcache}\n\n\n\n")

                # clearing file
                file_obj.seek(0)
                file_obj.truncate(0) # need '0' when using r+
                file_obj.writelines(self.update_fcache)

            else:
                with open(self.path, 'r') as file:
                    raw = file.readlines()
                for count, line in enumerate(raw):
                    if id_search in line:
                        raw[count] = f"{id}---{dic}\n"
                        break
                with open(self.path, 'w') as file:
                    file.writelines(raw)
                
        except FileNotFoundError:
            self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def re_id(self):  # reformat the file to have corrects id's
        try:
            with open(self.path, 'r') as file:
                contents = raw = file.readlines()

            for count, line in enumerate(raw):
                if f"{count}---" not in line:
                    contents[count] = f"{count}---{line.split('---')[1]}"

            with open(self.path, 'w') as file:
                file.writelines(contents)

        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def overwrite(self):  # overwrites database or creates new one if not exist
        if self.is_valid_path():  # check if path is valid
            open(self.path, 'w').close()  # open then close right after to make empty file and overwrites if already exist
            self.line_count = 0
        else:
            self.FileNotFoundError_func(f"Path is not valid: {self.path}")

    def fetch_by_key(self, akey, exact=True, fast=False):  # returns all dictionaries with matching keys
        try:
            with open(self.path, 'r') as file:
                dics = []
                raw = file.readlines()
                add = dics.append  # fast way of appending
            if exact == False and fast == True:  # fast mode
                dics = [ast.literal_eval(line.split('---')[1]) for line in raw if akey in line]

            elif exact == True and fast == False:
                for line in raw:
                    dic = ast.literal_eval(line.split('---')[1])
                    for key in dic:
                        if key == akey:
                            add(dic)
                            break

            elif exact == False and fast == False:
                for line in raw:
                    dic = ast.literal_eval(line.split('---')[1])
                    for key in dic:
                        if akey in key:
                            add(dic)
                            break

            else:
                self.TypeError_func("Invalid parametres for method.")
            return dics
        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def fetch_by_value(self, avalue, exact=True, fast=False):  # returns all dictionaries with matching values
        try:
            with open(self.path, 'r') as file:
                dics = []
                raw = file.readlines()
                add = dics.append  # fast way of appending
            if exact == False and fast == True:
                [add(ast.literal_eval(line.split('---')[1])) for line in raw if avalue in line]

            elif exact == True and fast == False:  # optimised since 1.0
                for line in raw:
                    dic = ast.literal_eval(line.split('---')[1])
                    for key, value in dic.items():
                        if avalue == value:
                            add(dic)
                            break

            elif exact == False and fast == False:
                for line in raw:
                    dic = ast.literal_eval(line.split('---')[1])
                    for key, value in dic.items():
                        if avalue in value:
                            add(dic)
                            break

            else:
                self.TypeError_func("Invalid parametres for method.")
            return dics
        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def clear(self):  # wipes db
        try:
            with open(self.path, 'w') as file:
                file.write('')
        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def quick_write(self, list):  # quickly writes every dictionary in list
        if str(type(list)) == "<class 'list'>":  # making sure the "list" is list
            try:
                with open(self.path, 'a') as file:
                    start_line_num = self.line_count
                    for count, dic in enumerate(list):
                        if str(type(dimn c)) == "<class 'dict'>":
                            if "---" not in str(dic):
                                line_num = start_line_num + count
                                file.write(f"{line_num}---{dic}\n")
                            else:
                                self.ValueError_func("'---' is an illegal set of characters.")
                        else:
                            self.TypeError_func(f"list must only contain dictionaries, not {str(type(dic))}")
                    self.line_count = line_num
            except FileNotFoundError:
                self.FileNotFoundError_func(
                    f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

        else:
            self.TypeError_func(f"quick_write only accepts lists, not {str(type(list))}")

    def quick_remove(self, alist):
        if str(type(alist)) == "<class 'list'>":  # making sure id is a list
            try:
                with open(self.path, 'r') as file:
                    contents = file.readlines()
                for count, line in enumerate(contents):
                    if int(line.split("---")[0]) in alist:
                        del contents[count]
                        self.line_count -= 1

                with open(self.path, 'w') as file:  # rewriting to file
                    file.writelines(contents)
                return
            except FileNotFoundError:
                self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")
        else:
            self.TypeError_func(f"Id has to be an integer or a list of integers, not {str(type(list))}")

    def id_by_value(self, avalue, fast=True):
        try:
            ids = []
            idAdd = ids.append
            with open(self.path, 'r') as file:
                raw = file.readlines()
            if fast:
                v_search = f": '{avalue}'" if str(type(avalue)) == "<class 'str'>" else f": {avalue}"
                ids = [count for count, line in enumerate(raw) if v_search in line]  # both these lines are fast af
            elif not fast:
                for count, line in enumerate(raw):
                    dic = ast.literal_eval(line.split("---")[1])
                    for key, value in dic.items():
                        if value == avalue:
                            idAdd(count)
                            break
            return ids
        except FileNotFoundError:
            self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def id_by_key(self, akey, fast=True):
        try:
            ids = []
            idAdd = ids.append
            if str(type(akey)) != "<class 'str'>":
                raise TypeError_func("Key needs to be string not ", str(type(akey)))
            with open(self.path, 'r') as file:
                raw = file.readlines()
            if fast:
                v_search = f"'{akey}': " if str(type(akey)) == "<class 'str'>" else f"{akey}: "
                ids = [count for count, line in enumerate(raw) if v_search in line]  # both these lines are fast af
            elif not fast:
                for count, line in enumerate(raw):
                    dic = ast.literal_eval(line.split("---")[1])
                    for key in dic:
                        if key == akey:
                            idAdd(count)
                            break
            return ids
        except FileNotFoundError:
            self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")
