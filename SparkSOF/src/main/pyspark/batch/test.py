# l = ['pyqt', 'import', 'qtcore', 'qtwidgets', 'class', 'mainwindow', 'qtwidgets', 'qmainwindow', 'def', 'init', 'self', 'parent', 'none', 'super', 'mainwindow', 'self', 'init', 'parent', 'self', 'graphicsview', 'qtwidgets', 'qgraphicsview', 'self', 'setcentralwidget', 'self', 'graphicsview', 'self', 'graphicsview', 'viewport', 'installeventfilter', 'self', 'self', 'self', 'def', 'eventfilter', 'self', 'obj', 'event', 'obj', 'self', 'graphicsview', 'viewport', 'event', 'type', 'qtcore', 'qevent', 'mousebuttondblclick', 'self', 'func', 'event', 'return', 'super', 'mainwindow', 'self', 'eventfilter', 'obj', 'event', 'def', 'func', 'self', 'event', 'print', 'event', 'po', 'self', 'self', 'name', 'main', 'import', 'sys', 'app', 'qtwidgets', 'qapplication', 'sys', 'argv', 'mainwindow', 'show', 'sys', 'exit', 'app', 'exec']
# lx = [pyqt, import, qtcore, qtwidgets, class, mainwindow, qtwidgets, qmainwindow, def, init, self, parent, none, super, mainwindow, self, init, parent, self, graphicsview, qtwidgets, qgraphicsview, self, setcentralwidget, self, graphicsview, self, graphicsview, viewport, installeventfilter, self, self, self, def, eventfilter, self, obj, event, obj, self, graphicsview, viewport, event, type, qtcore, qevent, mousebuttondblclick, self, func, event, return, super, mainwindow, self, eventfilter, obj, event, def, func, self, event, print, event, po, self, self, name, main, import, sys, app, qtwidgets, qapplication, sys, argv, mainwindow, show, sys, exit, app, exec]
# print(lx)
# print(l)

l = [311535874, 2105687172, 2535125380, 2496898438, 4116091528, 2717066761, 2199674893, 2035806864, 2453719184, 1979939346, 3003798419, 2124984089, 3342325659, 3902237852, 2279920668, 2401757343, 2097325729, 325614497, 5801638, 3012395302, 2603680169, 268081452, 2766072622, 1484149680, 894405048, 2878784186, 586880570, 2661251772, 1152308541, 1362524481, 273803841, 3338346050, 4207436100, 3823628355, 1781397190, 3493137857, 1761796678, 1026193609, 410179273, 4274873931, 3193521868, 2261968589, 2357219782, 3173534672, 2286252241, 1398145107, 3995256276, 1508342356, 3568003542, 3632833751, 3840172118, 3153769689, 2759913689, 57056219, 3650792540, 2965323640, 1417239646, 2931389022, 3899398112, 248555617, 972112229, 2504834919, 2764293352, 959905128, 1693819253, 1113708021, 337345911, 4110600566, 579385206, 3317898363, 777491576]
lx = [311535874, 2105687172, 2535125380, 2496898438, 4116091528, 2717066761, 2199674893, 2035806864, 2453719184, 1979939346, 3003798419, 1544607126, 2124984089, 3342325659, 3902237852, 2279920668, 2401757343, 2097325729, 5801638, 2603680169, 268081452, 2766072622, 3968987058, 2722796469, 894405048, 2878784186, 586880570, 2661251772, 1152308541, 1791549528, 1362524481, 273803841, 3338346050, 4207436100, 3823628355, 1781397190, 3493137857, 1761796678, 1026193609, 410179273, 4274873931, 3193521868, 2261968589, 2357219782, 3173534672, 2286252241, 1398145107, 3995256276, 1508342356, 3568003542, 3632833751, 3840172118, 3153769689, 2759913689, 57056219, 3650792540, 2965323640, 1417239646, 2931389022, 3899398112, 248555617, 2733950177, 972112229, 2504834919, 2764293352, 959905128, 1693819253, 1113708021, 337345911, 4110600566, 3317898363, 777491576, 1970189763]

print("Union :", set(l) | set(lx))

# intersection
print("Intersection :",set(l) & set(lx))

# difference
print("Difference :", set(l) - set(lx))

