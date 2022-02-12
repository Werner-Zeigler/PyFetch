import ast
import os


# ---VERSION 2.0---

class database:
    def __init__(self, Apath):  # setting object path to path
        self.path = Apath
        if self.is_valid_path():
            p = Apath.split('.')
            if p[1] != 'pf':
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
        has_cache = False
        with open(self.path, 'r') as file:
            lines = file.readlines()
            if len(lines) < 0:
                if "@cache<@" in lines[0]:
                    has_cache = True
        if has_cache:
            return len(lines) - 1  # -1 to account for cache on the first line
        elif not has_cache:
            return len(lines)

    def line_num_through_id(self, id):
        """returns the line number corresponding with the id"""
        has_cache = False
        with open(self.path, 'r') as file:
            lines = file.readlines()
            if "@cache<@" in lines[0]:
                has_cache = True
        for count, line in enumerate(lines):
            if int(line.split("---")[0]) == id:
                if has_cache:
                    return count - 1  # -1 to account for cache on the first line
                elif not has_cache:
                    return count
        self.IndexError_func(f"Id ({str(id)}) not found.")

    def is_valid_path(self):  # there must always be a dot for the extension, eg: .txt, .exe, .dll, .mp3
        if '.' not in self.path:
            return False
        else:
            return True

    def make(self):  # creates db if not exists
        if self.is_valid_path():  # check if path is valid
            if not os.path.exists(self.path):  # check if file exists, and if it doesn't:
                open(self.path, 'a').close()  # open then close right after to make empty file
        else:
            self.FileNotFoundError_func(f"Path is not valid: {self.path}")

    def insert(self, dic):  # inserts to db (has to be a dictionary)
        if str(type(dic)) == "<class 'dict'>":  # ensures input is a dictionary
            try:
                if "---" not in str(dic):
                    line_num = str(self.count_lines())
                    with open(self.path, 'a') as file:
                        file.write(f"{line_num}---{str(dic)}\n")
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
                with open(self.path, 'r') as file:
                    lines = file.readlines()
                for line in lines:
                    content = line.split('---')  # separating id and dictionary in line
                    if int(content[0]) == id:
                        return ast.literal_eval(content[1])
                self.IndexError_func(f"id ({str(id)}) not found")
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
                for line in contents:
                    line = line.split('---')
                    if int(line[0]) == id:
                        del contents[self.line_num_through_id(id)]
                        with open(self.path, 'w') as file:
                            for line in contents:
                                file.write(line)  # rewriting lines to file
                            return
                self.IndexError_func(f"Id ({str(id)}) not found.")
            except FileNotFoundError:
                self.FileNotFoundError_func(
                    f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def fetch_all(self):  # returns all dictionaries in a list
        try:
            with open(self.path, 'r') as file:
                raw = file.readlines()
            contents = [ast.literal_eval(line.split('---')[1]) for line in raw]  # return all dics line by line
            return contents
        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def fetch_all_raw(self):
        try:
            with open(self.path, 'r') as file:
                raw = file.readlines()
            contents = [line for line in raw]  # return all line by line
            return contents
        except FileNotFoundError:
            self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def update(self, id, dic):
        try:
            id_search = str(id)+"---"
            with open(self.path, 'r') as file:
                raw = file.readlines()
            for count, line in enumerate(raw):
                if id_search in line:
                    raw[count] = id_search + str(dic) + "\n"
                    break
            with open(self.path, 'w') as file:
                file.writelines(raw)
        except FileNotFoundError:
            self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def re_id(self):  # reformat the file to have corrects id's
        try:
            with open(self.path, 'r') as file:
                raw = file.readlines()
                contents = []
            for count, line in enumerate(raw):
                if f"{str(count)}---" in line:
                    contents.append(str(count) + "---" + line.split("---")[1])
            with open(self.path, 'w') as file:
                for line in contents:
                    file.write(line)
        except FileNotFoundError:
            self.FileNotFoundError_func(
                f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def overwrite(self):  # overwrites database or creates new one if not exist
        if self.is_valid_path():  # check if path is valid
            open(self.path,
                 'w').close()  # open then close right after to make empty file and overwrites if already exist
        else:
            self.FileNotFoundError_func(f"Path is not valid: {self.path}")

    def fetch_by_key(self, akey, exact=True, fast=False):  # returns all dictionaries with matching keys
        try:
            with open(self.path, 'r') as file:
                dics = []
                raw = file.readlines()
                add = dics.append  # fast way of appending
            if exact == False and fast == True:  # fast mode
                [add(ast.literal_eval(line.split('---')[1])) for line in raw if akey in line]

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
                    start_line_num = self.count_lines()
                    for count, dic in enumerate(list):
                        if str(type(dic)) == "<class 'dict'>":
                            if "---" not in str(dic):
                                file.write(f"{start_line_num + count}---{dic}\n")
                            else:
                                self.ValueError_func("'---' is an illegal set of characters.")
                        else:
                            self.TypeError_func(f"list must only contain dictionaries, not {str(type(dic))}")
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
                    if int(line.split("---")[0] in alist):
                        del contents[count]

                with open(self.path, 'w') as file:  # rewriting to file
                    file.writelines(contents)
                return
            except FileNotFoundError:
                self.FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")
        else:
            self.TypeError_func(f"Id has to be an integer or a list of integers, not {str(type(list))}")

    def id_by_value(self, avalue, fast=False):
        try:
            ids = []
            idAdd = ids.append
            if str(type(avalue)) != "<class 'str'>":
                raise TypeError_func("Value needs to be string not ", str(type(avalue)))
            with open(self.path, 'r') as file:
                raw = file.readlines()
            if fast:
                vSearch = f"': '{avalue}'"
                for count, line in enumerate(raw):
                    if vSearch in line:
                        idAdd(count)
            elif not fast:
                for count, line in enumerate(raw):
                    dic = ast.literal_eval(line.split("---")[1])
                    for key, value in dic.items():
                        if value == avalue:
                            idAdd(count)
                            break
            return ids
        except FileNotFoundError:
            FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")

    def id_by_key(self, akey, fast=False):
        try:
            ids = []
            idAdd = ids.append
            if str(type(akey)) != "<class 'str'>":
                raise TypeError_func("Key needs to be string not ", str(type(akey)))
            with open(self.path, 'r') as file:
                raw = file.readlines()
            if fast:
                vSearch = f"': '{akey}'"  # quickly search if this is in line for every line
                for count, line in enumerate(raw):
                    if vSearch in line:
                        idAdd(count)
            elif not fast:
                for count, line in enumerate(raw):
                    dic = ast.literal_eval(line.split("---")[1])
                    for key in dic:
                        if key == akey:
                            idAdd(count)
                            break
            return ids
        except FileNotFoundError:
            FileNotFoundError_func(f"The file doesn't exist or wasn't found, use the make() method to make a new database.\n{self.path}")