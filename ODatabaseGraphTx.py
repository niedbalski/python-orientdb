from jpype import *
import os

class   ODatabaseGraphTxException(Exception):
    pass

class   OGraphVertex:
    pass


class   ODatabaseGraphTx:

    def __init__(self, name, database, username, password):
        try:
            self.name     = name
            self.jarpath  = os.path.join(os.path.dirname(
                os.path.abspath(__file__) ), 'lib', 'orientdb')

            #start the JVM
            if not isJVMStarted():
                startJVM(getDefaultJVMPath(), "-Djava.ext.dirs=%s" % self.jarpath)
           
            #load the orientdb graph java package
            self.package = JPackage('com.orientechnologies.orient.core.db.graph')
            self.database  = self.package.ODatabaseGraphTx(database)
            
            #Open a connection to the database
            self.database.open(username, password)
    
        except (Exception), e:
            raise ODatabaseGraphTxException("%s" % e)

    def __del__(self):
        try:
            self.database.close()
            #force shutdown the JVM
            shutdownJVM()
        except (Exception), e:
            raise ODatabaseGraphTxException("%s" % e)

    def createVertex(self, attributes=None):
        try:
            vertex = self.database.createVertex()
            if attributes is not None:
                for attr, value in attributes.iteritems():
                    vertex.set(attr, value)
            return(vertex)
        except (Exception), e:
            raise ODatabaseGraphTxException("%s" % e)
    
    def setRootVertex(self, vertex):
        try:
            return self.database.setRoot(self.name, vertex)
        except (Exception), e:
            raise ODatabaseGraphTxException("%s" % e)

    def getRootVertex(self):
        try:
            return self.database.getRoot(self.name)
        except (Exception), e:
            raise ODatabaseGraphTxException("%s" % e)
    
if __name__ == "__main__":

    db = ODatabaseGraphTx('graph', 'local:/usr/local/databases/graph', 'admin', 'admin')
    attributes = { 'name' : "jorge", 'lastname' : "niedbalski" }

    vertex1 = db.createVertex(attributes)
    vertex2 = db.createVertex()

    db.setRootVertex(vertex1)
    
    edge = vertex1.link(vertex2)
    edge.set('foo', 'bar')
    edge.save()

    for v in db.getRootVertex().outEdges:
        print v

