#
# Plugin to collectd statistics from beanstalkd
#

import collectd, socket

class Beanstalk(object):

    def __init__(self):
        self.plugin_name = "beanstalkd"
        self.host = '127.0.0.1'
        self.port = 11300
        self.tubes_prefix = ['default']

    def submit(self, type, instance, value, tube=None):
        if tube:
            plugin_instance = '%s-%s' % (self.port, tube)
        else:
            plugin_instance = str(self.port)

        v = collectd.Values()
        v.plugin = self.plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()

    def yaml_parse(self, data):
        lines = [l.split(": ") for l in data.split("\n")[1:]]
        return dict([l for l in lines if len(l) == 2])

    def tubes_parse(self, data):
        return [l[2:] for l in data.split("\n")[1:] if len(l) > 2]
        

    def interact(self, cmd, expects):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.host, self.port))
        sock.sendall(cmd)
        sock_file = sock.makefile('rb')
        line = sock_file.readline()
        if line:
            status, result_len = line.split()
            if expects.index(status) > -1:
                body = sock_file.read(int(result_len))
                sock_file.read(2)
                return body
        sock.close()

    def do_server_status(self):
        srv_stats = self.yaml_parse(self.interact('stats\r\n', ['OK']))
        print "srv_stats: %r" % srv_stats
        for cmd in ('put', 'reserve-with-timeout', 'delete'):
            self.submit('counter', cmd, srv_stats['cmd-%s' % cmd])
        self.submit('counter', 'total_jobs', srv_stats['total-jobs'])
        self.submit('gauge', 'current_tubes', srv_stats['current-tubes'])
        self.submit('connections', 'connections', srv_stats['current-connections'])
        
        tubes = self.tubes_parse(self.interact('list-tubes\r\n', ['OK']))
        for tube in tubes:
            for prefix in self.tubes_prefix:
                if tube.startswith(prefix):
                    tube_stats = self.yaml_parse(self.interact('stats-tube %s\r\n' % tube, ['OK']))
                    self.submit('records', 'current_ready', tube_stats['current-jobs-ready'], tube)
                    self.submit('counter', 'total_jobs', tube_stats['total-jobs'], tube)

    def config(self, obj):
        for node in obj.children:
            if node.key == 'Port':
                self.port = int(node.values[0])
            elif node.key == 'Host':
                self.host = node.values[0]
            elif node.key == 'tubes_prefix':
                self.tubes_prefix = node.values
            else:
                collectd.warning("beanstalkd plugin: Unkown configuration key %s" % node.key)

beanstalkd = Beanstalk()
collectd.register_read(beanstalkd.do_server_status)
collectd.register_config(beanstalkd.config)
