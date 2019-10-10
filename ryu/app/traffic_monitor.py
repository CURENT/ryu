from operator import attrgetter

from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from datetime import datetime
import time
import json
import logging
import csv


logger_t = logging.getLogger(__name__)
logger_t.setLevel(logging.INFO)
fh = logging.FileHandler('network_stats.json')
logger_t.addHandler(fh)


class SimpleMonitor13(simple_switch_13.SimpleSwitch13):

    def __init__(self, *args, **kwargs):
        super(SimpleMonitor13, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)
        self.flow_stats_csv = 'net_stats_flow.csv'
        self.port_stats_csv = 'net_stats_port.csv'
        with open(self.flow_stats_csv, 'w') as f:
            f.write('epoch-time,datapath,in-port,eth-dest,out-port,packets,bytes\n')
        with open(self.port_stats_csv, 'w') as f:
            f.write('epoch-time,datapath,port,rx-pkts,rx-bytes,rx-error,tx-pkts,tx-bytes,tx-error\n')


    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(2)

    def _request_stats(self, datapath):
        self.logger.debug('%s send stats request: %016x',
                    datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body

        # msg_dict = ev.msg.to_jsondict()
        # msg_dict['timestamp'] = time.time()
        # msg_dict['dpid'] = ev.msg.datapath.id
        #
        # logger_t.info('%s', json.dumps(msg_dict, ensure_ascii=False,
        #                                   indent=2, sort_keys=True))

        # self.logger.info('\n--> {} Flow Stats:'.format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
        # self.logger.info('datapath         '
        #                  'in-port  eth-dst           '
        #                  'out-port packets  bytes')
        # self.logger.info('---------------- '
        #                  '-------- ----------------- '
        #                  '-------- -------- --------')

        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):

            # self.logger.info('%016x %8x %17s %8x %8d %8d',
            #                  ev.msg.datapath.id,
            #                  stat.match['in_port'], stat.match['eth_dst'],
            #                  stat.instructions[0].actions[0].port,
            #                  stat.packet_count, stat.byte_count)

            with open(self.flow_stats_csv, 'a') as f:
                out_line = ('%.4f,%016x,%8x,%17s,%8x,%8d,%8d'%(time.time(), ev.msg.datapath.id,
                                                             stat.match['in_port'], stat.match['eth_dst'],
                                                             stat.instructions[0].actions[0].port,
                                                             stat.packet_count, stat.byte_count))
                f.write(out_line + '\n')

                self.logger.info('flow stats written')

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body

        # msg_dict = ev.msg.to_jsondict()
        # msg_dict['timestamp'] = time.time()
        # msg_dict['dpid'] = ev.msg.datapath.id
        # logger_t.info('%s', json.dumps(msg_dict, ensure_ascii=False,
        #                                   indent=2, sort_keys=True))

        # self.logger.info('\n--> {} Port Stats:'.format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
        # self.logger.info('datapath         port     '
        #                  'rx-pkts  rx-bytes rx-error '
        #                  'tx-pkts  tx-bytes tx-error')
        # self.logger.info('---------------- -------- '
        #                  '-------- -------- -------- '
        #                  '-------- -------- --------')

        for stat in sorted(body, key=attrgetter('port_no')):
            # self.logger.info('%016x %8x %8d %8d %8d %8d %8d %8d',
            #                  ev.msg.datapath.id, stat.port_no,
            #                  stat.rx_packets, stat.rx_bytes, stat.rx_errors,
            #                  stat.tx_packets, stat.tx_bytes, stat.tx_errors)

            with open(self.port_stats_csv, 'a') as f:
                f.write('%.4f %016x %8x %8d %8d %8d %8d %8d %8d' %(time.time(),
                        ev.msg.datapath.id, stat.port_no,
                        stat.rx_packets, stat.rx_bytes, stat.rx_errors,
                        stat.tx_packets, stat.tx_bytes, stat.tx_errors) + '\n')
                self.logger.info('port stats written')

