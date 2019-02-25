import helper

code = """from PyQt5 import QtCore, QtWidgets

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)

        self.graphicsView = QtWidgets.QGraphicsView()
        self.setCentralWidget(self.graphicsView)
        self.graphicsView.viewport().installEventFilter(self)

        self.in_1 = 10
        self.in_2 = 20

    def eventFilter(self, obj, event):
        if obj is self.graphicsView.viewport():
            if event.type() == QtCore.QEvent.MouseButtonDblClick:
                self.func(event)
        return super(MainWindow, self).eventFilter(obj, event)

    def func(self, event):
        print(event.pos(), self.in_1, self.in_2)


if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec_())"""


processed_code = helper.get_processed_code(code)
print("===========================================")
print(processed_code)
print("===========================================")
shingles_in_code = helper.generate_shingles(processed_code)
print("===========================================")
print(shingles_in_code)
print("===========================================")

# [pyqt, import, qtcore, qtwidgets, class, mainwindow, qtwidgets, qmainwindow, def,
# init, self, parent, none, super, mainwindow, self, init, parent, self, graphicsview,
# qtwidgets, qgraphicsview, self, setcentralwidget, self, graphicsview, self, graphicsview, viewport,
# installeventfilter, self, self, self, def, eventfilter, self, obj, event, obj, self, graphicsview, viewport,
# event, type, qtcore, qevent, mousebuttondblclick, self, func, event, return, super, mainwindow,
# self, eventfilter, obj, event, def, func, self, event, print, event, po, self, self, name, main, import, sys, app,
# qtwidgets, qapplication, sys, argv, mainwindow, show, sys, exit, app, exec]
