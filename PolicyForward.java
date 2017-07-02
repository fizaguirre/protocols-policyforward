package net.floodlightcontroller.policyforward;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.projectfloodlight.openflow.protocol.OFFlowModCommand;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.*;
import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.UpdateOperation;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.*;
import net.floodlightcontroller.routing.IRoutingService.PATH_METRIC;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.statistics.SwitchPortBandwidth;
import net.floodlightcontroller.topology.ITopologyManagerBackend;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.TopologyManager;

public class PolicyForward extends ForwardingBase implements IOFMessageListener, IFloodlightModule, ILinkDiscoveryListener {
	
	protected static Logger logger;
	protected BlockingQueue<LDUpdate> lduUpdate; //Queue to hold pending topology updates
	protected static ILinkDiscoveryService linkDiscoveryService; //LLDP service. It handles the LLDP protocol. We need to subscribe to it to listen for LLDP topology events.
	protected ITopologyService topologyService;
	protected IRoutingService routingManager;
	protected IStatisticsService statisticsService;
	protected int count = 0;

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
						
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
				
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		
		//Depends on IFloodlightService and ILinkDiscoveryService
		Collection<Class<? extends IFloodlightService>> l = 
				new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(ILinkDiscoveryService.class);
		l.add(ITopologyService.class);
		l.add(IRoutingService.class);
		l.add(IOFSwitchService.class);
		l.add(IStatisticsService.class);
		
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		super.init();
		this.floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		this.switchService = context.getServiceImpl(IOFSwitchService.class);
		logger = LoggerFactory.getLogger(PolicyForward.class);
		linkDiscoveryService = context.getServiceImpl(ILinkDiscoveryService.class);
		topologyService = context.getServiceImpl(ITopologyService.class);
		routingManager = context.getServiceImpl(IRoutingService.class);
		statisticsService = context.getServiceImpl(IStatisticsService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		super.startUp();
		logger.info("Starting up PolicyForward module");
		linkDiscoveryService.addListener(this);
		lduUpdate = new LinkedBlockingQueue<LDUpdate>();
		routingManager.setMaxPathsToCompute(10);
		statisticsService.collectStatistics(true);

	}

	@Override
	public net.floodlightcontroller.core.IListener.Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			IRoutingDecision decision, FloodlightContext cntx) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		//logger.info("Packet in type: {}", msg.getType());
		
		//logger.info("Statistics: {}", statisticsService.getBandwidthConsumption().toString());
		;

		switch(msg.getType())
		{
		case PACKET_IN:
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			//logger.info("Packet from src mac addr {} received from switch {}", eth.getSourceMACAddress(), sw.getId().toString());
			//logger.info("Packet eth payload: {}", eth.getPayload());
			
			if ( eth.getEtherType() == EthType.ARP) {
				// SEND PACKET TO ALL OTHER PORTS OF THE SWITCH
				
				ARP arp = (ARP) eth.getPayload();
				if ( arp.getOpCode() == ARP.OP_REQUEST) {
					this.doBroacastPacket(sw, msg);
				}
					
				else if ( arp.getOpCode() == ARP.OP_REPLY) {
					
					this.doFlow(sw, msg, cntx, eth);			
				}
			}
			else if (eth.getEtherType() == EthType.IPv4 ) {
				logger.info("Packet from src mac addr {} received from switch {}", eth.getSourceMACAddress(), sw.getId().toString());
				logger.info("Packet eth payload: {}", eth.getPayload());
				this.doFlow(sw, msg, cntx, eth);
			}
			
			break;
		default:
			break;
		}
		
		return Command.CONTINUE;
	}
	
	private void doFlow(IOFSwitch sw, OFMessage msg, FloodlightContext cntx, Ethernet eth) {
		IDevice dstHost = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE);
		IDevice srcHost = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_SRC_DEVICE);
		
		SwitchPort destPortSwitch = null;
		for ( SwitchPort swp : dstHost.getAttachmentPoints()) {
			if (topologyService.isEdge(swp.getNodeId(), swp.getPortId())) {
				destPortSwitch = swp;
			}
		}
		
		if (destPortSwitch == null) {
			logger.info("No switch found connecting the node.");
			return;
		}
		
		
		OFPacketIn pi = (OFPacketIn) msg;

		Path path = routingManager.getPath(sw.getId(), pi.getInPort(), destPortSwitch.getNodeId(), destPortSwitch.getPortId());
		logger.info("Pacote de {} para {}", sw.getId().toString(), destPortSwitch.getNodeId().toString());
		List<Path> paths = routingManager.getPathsFast(sw.getId(), destPortSwitch.getNodeId());
		NodePortTuple n_src = new NodePortTuple(sw.getId(), pi.getInPort());
		NodePortTuple n_dst = new NodePortTuple(destPortSwitch.getNodeId(), destPortSwitch.getPortId());
		Map<NodePortTuple, SwitchPortBandwidth> m = statisticsService.getBandwidthConsumption();
		//Usando 10M como referencia para "link congestionado"
		U64 dez_megabits = U64.of(10000000);
		Path escolhido = null;
		
		for (Path pt : paths) {
			escolhido = pt;
			List<NodePortTuple> lista = escolhido.getPath();
			if(!lista.get(0).equals(n_src)) {
				lista.add(0, n_src);
				lista.add(n_dst);
			}
			logger.info("OLhando path: {}", lista.toString());
			for (int i = 1; i < lista.size(); i+=2) {
				NodePortTuple nodePortTuple = lista.get(i);
				if (topologyService.isEdge(nodePortTuple.getNodeId(), nodePortTuple.getPortId()))
					continue;	
				
				logger.info("OLhando link: {}", nodePortTuple.toString());	
				SwitchPortBandwidth spb = m.get(nodePortTuple);
				//Tx > que 10M
				if(spb != null)
				if(dez_megabits.compareTo(spb.getBitsPerSecondTx()) <= 0) {
					//caminho congestionado
					logger.info("{} -- {}", dez_megabits, spb.getBitsPerSecondTx());
					escolhido = null;
				}
				if(escolhido != null) {
					logger.info("CAMINHO Escolhido de {}!!!",paths.size());
					break;
				}
			}
		}
			
		if(escolhido == null) {
			logger.info("NENHUM CAMINHO ENCONTRADO");
			escolhido = path;
		}
		
		logger.info("Path found: {}",escolhido.toString());
		
		
		Match matchRule = buildMatch(sw, msg, cntx, eth);
		
		pushRoute(escolhido, matchRule, pi, sw.getId(), DEFAULT_FORWARDING_COOKIE, cntx, false, OFFlowModCommand.ADD);
	}
	
	private Match buildMatch(IOFSwitch sw, OFMessage msg, FloodlightContext cntx, Ethernet eth) {
		IDevice dstHost = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE);
		IDevice srcHost = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_SRC_DEVICE);
		
		Match matchRule = null;
		
		if (eth.getEtherType() == EthType.ARP) {
			logger.info("Building ARP match.");
			matchRule = sw.getOFFactory().buildMatch()
				.setExact(MatchField.ETH_TYPE, EthType.ARP)
				.setExact(MatchField.ETH_SRC, srcHost.getMACAddress())
				.setExact(MatchField.ETH_DST, dstHost.getMACAddress())
				.build();
		}else if (eth.getEtherType() == EthType.IPv4) {
			logger.info("Building IVv4 match.");
			IPv4 ipv4 = (IPv4) eth.getPayload();
			
			
			matchRule = sw.getOFFactory().buildMatch()
				.setExact(MatchField.ETH_TYPE, EthType.IPv4)
				.setExact(MatchField.ETH_SRC, srcHost.getMACAddress())
				.setExact(MatchField.ETH_DST, dstHost.getMACAddress())
				.setExact(MatchField.IPV4_SRC, ipv4.getSourceAddress())
				.setExact(MatchField.IPV4_DST, ipv4.getDestinationAddress())
				.build();
			
		}
		
		return matchRule;
	}
	
	private void doBroacastPacket(IOFSwitch sw, OFMessage m) {
		OFPacketIn pi = (OFPacketIn) m;
		OFPort portIn = pi.getInPort();
		
		OFPacketOut po;
		Set<OFPort> broadcastPorts;
		broadcastPorts = topologyService.getSwitchBroadcastPorts(sw.getId());
		
        if (broadcastPorts.isEmpty()) {
            log.info("No broadcast ports found. Using FLOOD output action");
            broadcastPorts = Collections.singleton(OFPort.FLOOD);
        }
		for (OFPort port : broadcastPorts) {
			//logger.info("Looking at port : {}", port.toString());
			
			if( port == portIn)
				continue;
			
			po = sw.getOFFactory().buildPacketOut()
					.setData(pi.getData())
					.setInPort(portIn)
					.setActions(Collections.singletonList((OFAction) sw.getOFFactory().actions().output(port, Integer.MAX_VALUE)))
					.build();
			sw.write(po);		
		}

		return;
	}
	
	@Override
	public String getName(){
		return PolicyForward.class.getSimpleName();
	}

	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
		
		lduUpdate.addAll(updateList); //Get a list of link layer discovery updates
		/*
		for (LDUpdate u : updateList) {
            if (u.getOperation() == UpdateOperation.LINK_REMOVED ||
                    u.getOperation() == UpdateOperation.PORT_DOWN) {
            	//pacotes que serao enviados para apagar flows
            	Set<OFMessage> msgs = new HashSet<OFMessage>();
            	if (u.getSrc() != null && !u.getSrc().equals(DatapathId.NONE)) {
            		IOFSwitch srcSw = switchService.getSwitch(u.getSrc());
            		msgs.add(srcSw.getOFFactory().buildFlowDelete()
                            .setMatch(srcSw.getOFFactory().buildMatch()
                                    .setExact(MatchField.IN_PORT, u.getSrcPort())
                                    .build())
                            .build());
                    msgs.add(srcSw.getOFFactory().buildFlowDelete()
                            .setOutPort(u.getSrcPort())
                            .build());
                    
                    Set<DatapathId> switchs =  switchService.getAllSwitchDpids();
                    for (DatapathId sw_id : switchs) {
						IOFSwitch sw = switchService.getSwitch(sw_id);
						if(sw != null && !sw.equals(srcSw)) {
                            msgs.add(sw.getOFFactory().buildFlowDelete()
                                    .setMatch(sw.getOFFactory().buildMatch()
                                            .setExact(MatchField.IN_PORT, npt.getPortId())
                                            .build())
                                    .build());
						}
						}
					}
                    messageDamper.write(srcSw, msgs);
            }
		}
		
	}*/
	}
	

}
