def translit(cyr:str):
    """Take a string in cyrillic script and return its counterpart in Latin."""
    if cyr is None:
        return None
    alpha = {'а':'a', 'б':'b', 'в':'v', 'г':'g', 'д':'d', 'е':'e', 'йе':'ye', 
             'ё':'yo', 'ж':'zh', 'з':'z', 'и':'i', 'й':'y', 'к':'k', 'л':'l', 
             'м':'m', 'н':'n', 'о':'o', 'п':'p', 'р':'r', 'с':'s', 'т':'t', 
             'у':'u', 'ф':'f', 'х':'kh', 'ц':'ts', 'ч':'ch', 'ш':'sh', 
             'щ':'sch', 'ь':'', 'ы':'y', 'ъ':'\'', 'э':'e', 'ю':'yu', 'я':'ya'}
    vowels = [' ','а','е','ё','и','о','у','ь','ы','ъ','э','ю','я']
    eng = ''
    prevchars = ' '+cyr[:-1].lower()
    nextchars = cyr[1:]+' '
    for idx, letter in enumerate(cyr):
        code = letter.lower()
        if code in alpha:
            if code == 'е':
                if prevchars[idx] in vowels:
                    code = 'йе'
            if letter.islower():
                e = alpha[code]
            elif nextchars[idx].islower():
                e = alpha[code].title()
            else:
                e = alpha[code].upper()
        else:
            e = letter
        eng = eng + e
    return eng