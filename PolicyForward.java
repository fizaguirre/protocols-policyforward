package net.floodlightcontroller.policyforward;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.projectfloodlight.openflow.protocol.OFFlowModCommand;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFStatsReply;
import org.projectfloodlight.openflow.protocol.OFStatsRequest;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.ICMPv4Type;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
import net.floodlightcontroller.linkdiscovery.Link;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.*;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.statistics.SwitchPortBandwidth;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.TopologyManager;

public class PolicyForward extends ForwardingBase implements IOFMessageListener, IFloodlightModule, ILinkDiscoveryListener {
	
	protected static Logger logger;
	protected BlockingQueue<LDUpdate> lduUpdate; //Queue to hold pending topology updates
	protected static ILinkDiscoveryService linkDiscoveryService; //LLDP service. It handles the LLDP protocol. We need to subscribe to it to listen for LLDP topology events.
	protected ITopologyService topologyService;
	protected IRoutingService routingManager;
	protected IStatisticsService statisticservice;

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
		statisticservice = context.getServiceImpl(IStatisticsService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		super.startUp();
		logger.info("Starting up PolicyForward module");
		linkDiscoveryService.addListener(this);
		lduUpdate = new LinkedBlockingQueue<LDUpdate>();
		routingManager.setMaxPathsToCompute(10);
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			IRoutingDecision decision, FloodlightContext cntx) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		logger.info("Packet in type: {}", msg.getType());
		switch(msg.getType())
		{
		case PACKET_IN:
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			logger.info("Packet from src mac addr {} received from switch {}", eth.getSourceMACAddress(), sw.getId().toString());
			logger.info("Packet eth payload: {}", eth.getPayload());
			
			if ( eth.getEtherType() == EthType.ARP) {
				// SEND PACKET TO ALL OTHER PORTS OF THE SWITCH
				
				ARP arp = (ARP) eth.getPayload();
				if ( arp.getOpCode() == ARP.OP_REQUEST)
					this.doBroacastPacket(sw, msg);
				else if ( arp.getOpCode() == ARP.OP_REPLY) {
					this.doFlow(sw, msg, cntx, eth);			
				}
			}
			else if (eth.getEtherType() == EthType.IPv4 ) {
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
		
		//List<Path> pathSlow = routingManager.getPathsSlow(sw.getId(), destPortSwitch.getNodeId(), 10);
		/*
		List<OFStatsReply> stats = this.getPortStatistcs(sw, pi.getInPort());
		
		while( stats == null) {
			//logger.info("no stats");
		}
		logger.info("{}", stats.toString());
		
		statisticservice.collectStatistics(true);
		SwitchPortBandwidth spb;
		for (Path p: pathSlow) {
			for (NodePortTuple npt : p.getPath()) {
				try {
				spb = statisticservice.getBandwidthConsumption(npt.getNodeId(), npt.getPortId());
				logger.info("Sw {} Bandwidth: {}", spb.getSwitchId(), spb.getBitsPerSecondRx().getValue());
				logger.info("LSBPS {}", spb.getLinkSpeedBitsPerSec().getValue());
				} catch (NullPointerException e) {
					logger.info("spb null");
				}
			}
		}
		*/
		
		Path path = routingManager.getPath(sw.getId(), pi.getInPort(), destPortSwitch.getNodeId(), destPortSwitch.getPortId());
		logger.info("Path found {}", path);
		
		Match matchRule = buildMatch(sw, msg, cntx, eth);
		
		pushRoute(path, matchRule, pi, sw.getId(), DEFAULT_FORWARDING_COOKIE, cntx, false, OFFlowModCommand.ADD);
	}
	
	protected List<OFStatsReply> getPortStatistcs(IOFSwitch sw, OFPort ofp) {		
		Match match;
		OFStatsRequest<?> req = null;
		ListenableFuture<?> future;
		List<OFStatsReply> values = null;
		
		match = sw.getOFFactory().buildMatch().build();
		
		req = sw.getOFFactory().buildPortStatsRequest()
				.setPortNo(ofp)
				.build();
		
		try {
			if ( req != null ) {
				future = sw.writeStatsRequest(req);
				//values = (List<OFStatsReply>) future.get(10 / 2, TimeUnit.SECONDS);
				values = (List<OFStatsReply>) future.get();
			}
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		return values;
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
		
		if ( broadcastPorts.isEmpty() ) { //Only one switch
			po = sw.getOFFactory().buildPacketOut()
			.setData(pi.getData())
			.setInPort(pi.getInPort())
			.setActions(Collections.singletonList((OFAction) sw.getOFFactory().actions().output(OFPort.FLOOD, Integer.MAX_VALUE)))
			.build();
			sw.write(po);
		}
		else {		
			for (OFPort port : broadcastPorts) {
				
				if( port == portIn)
					continue;
				
				po = sw.getOFFactory().buildPacketOut()
						.setData(pi.getData())
						.setInPort(portIn)
						.setActions(Collections.singletonList((OFAction) sw.getOFFactory().actions().output(port, Integer.MAX_VALUE)))
						.build();
				sw.write(po);			
			}
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
	}
	

}
