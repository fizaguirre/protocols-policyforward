package net.floodlightcontroller.policyforward;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.projectfloodlight.openflow.protocol.OFFlowModCommand;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPortConfig;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortFeatures;
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
import org.projectfloodlight.openflow.types.U64;
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
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.UpdateOperation;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.*;
import net.floodlightcontroller.routing.IRoutingService.PATH_METRIC;
import net.floodlightcontroller.statistics.IStatisticsService;
import net.floodlightcontroller.statistics.SwitchPortBandwidth;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyManagerBackend;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.TopologyManager;
import net.floodlightcontroller.util.OFMessageUtils;

public class PolicyForward extends ForwardingBase implements IOFMessageListener, IFloodlightModule, ILinkDiscoveryListener {
	
	protected static Logger logger;
	protected BlockingQueue<LDUpdate> lduUpdate; //Queue to hold pending topology updates
	protected static ILinkDiscoveryService linkDiscoveryService; //LLDP service. It handles the LLDP protocol. We need to subscribe to it to listen for LLDP topology events.
	protected ITopologyService topologyService;
	protected IRoutingService routingManager;
	protected IStatisticsService statisticsService;
	protected int count = 0;
	
	private static ScheduledFuture<?> futureCollector;
	private static IThreadPoolService threadPoolService;
	protected int interval = 5;
	private static final int HIST_SIZE = 10;
	private ConcurrentHashMap<NodePortTuple, LinkedBlockingQueue<U64>> history;

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
		
		l.add(IThreadPoolService.class);
		
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
		
		threadPoolService = context.getServiceImpl(IThreadPoolService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		super.startUp();
		logger.info("Starting up PolicyForward module");
		linkDiscoveryService.addListener(this);
		lduUpdate = new LinkedBlockingQueue<LDUpdate>();
		routingManager.setMaxPathsToCompute(2);
		statisticsService.collectStatistics(true);
		
		history = new ConcurrentHashMap<NodePortTuple, LinkedBlockingQueue<U64>>();
		futureCollector = threadPoolService.getScheduledExecutor().scheduleAtFixedRate(new CollectStats(), interval, interval, TimeUnit.SECONDS);

	}

	@Override
	public net.floodlightcontroller.core.IListener.Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			IRoutingDecision decision, FloodlightContext cntx) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		/* Handles the packet in event and take two actions.
		 * If the packet is an ARP request, flood as broadcast.
		 * If the packet is an IPv4 packet, than computes the path
		 * between the nodes and insert it into the openflow switches.
		 */
		
		switch(msg.getType())
		{
		case PACKET_IN:
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			
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

		Path path = routingManager.getPath(sw.getId(), OFMessageUtils.getInPort(pi), destPortSwitch.getNodeId(), destPortSwitch.getPortId());
		logger.info("Packet from {} to {}", sw.getId().toString(), destPortSwitch.getNodeId().toString());
		
		Path escolhido = null;

		if (sw.getId() != destPortSwitch.getNodeId())
			escolhido = this.getBestPathSlow(sw, msg, cntx, eth, destPortSwitch);
		else
			escolhido = routingManager.getPath(sw.getId(), OFMessageUtils.getInPort(pi), destPortSwitch.getNodeId(), destPortSwitch.getPortId());
		
		if(escolhido == null) {
			logger.info("NO PATH FOUND");
			escolhido = path;
		}
		
		logger.info("===> Path chosed: {} | {}",escolhido.getPathIndex(), escolhido.toString());
		
		Match matchRule = buildMatch(sw, msg, cntx, eth);
		
		pushRoute(escolhido, matchRule, pi, sw.getId(), DEFAULT_FORWARDING_COOKIE, cntx, false, OFFlowModCommand.ADD);
	}
	
	protected Path getBestPath(IOFSwitch sw, OFMessage msg, FloodlightContext cntx, Ethernet eth, SwitchPort destPortSwitch) {
		
		OFPacketIn pi = (OFPacketIn) msg;
		
		List<Path> paths = routingManager.getPathsFast(sw.getId(), destPortSwitch.getNodeId());
		NodePortTuple n_src = new NodePortTuple(sw.getId(), OFMessageUtils.getInPort(pi));
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
					//logger.info("CAMINHO Escolhido de {}!!!",paths.size());
					break;
				}
			}
		}
			
		if(escolhido == null) {
			logger.info("NENHUM CAMINHO ENCONTRADO");
			//escolhido = null;
		}
		
		return escolhido;
	}
	
	//Get the path using the slow function that computes the Yen's algorithm
	protected Path getBestPathSlow(IOFSwitch sw, OFMessage msg, FloodlightContext cntx, Ethernet eth, SwitchPort destPortSwitch) {
		OFPacketIn pi = (OFPacketIn) msg;
		
		
		List<Path> pathSlow = routingManager.getPathsSlow(sw.getId(), destPortSwitch.getNodeId(), 10);
		Long alpha = (long) 10000;
		Long hops;
		Long best = Long.MAX_VALUE;
		Long utilization;
		
		Path chosenPath = null;
		for (Path p : pathSlow ) {
			logger.info("{}", p.toString());
			utilization = Long.valueOf(0);
			long ponderada = 0;
			long peso = 1;
			long div = 1;
			if(p != null)
			{
				hops = Long.valueOf(p.getPath().size());
				peso = 1;
				div = 1;
			for (NodePortTuple npt : p.getPath()) {
				if(history != null && npt != null && history.get(npt) != null)
				for ( U64 util : history.get(npt)) {
					ponderada += peso * util.getValue();
					div += peso;
					peso++;
				}
				utilization = utilization + (ponderada / div);
			}

			utilization += hops * alpha;
			
			if (utilization < best ) {
				best = utilization;
				chosenPath = p;
			}
			logger.info("Path Candidate {} Utilization {}", p.getId(), utilization);
			}
			
		}
		
		if(chosenPath != null) {
			NodePortTuple n_src = new NodePortTuple(sw.getId(), OFMessageUtils.getInPort(pi));
			NodePortTuple n_dst = new NodePortTuple(destPortSwitch.getNodeId(), destPortSwitch.getPortId());
			if(n_src.getNodeId() != n_dst.getNodeId()) {
				List<NodePortTuple> lista = chosenPath.getPath();
				if(!lista.get(0).equals(n_src)) {
					lista.add(0, n_src);
					lista.add(n_dst);
				}					
			}
		}

		return chosenPath;
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
		
		//Build ARP match
		if (eth.getEtherType() == EthType.ARP) {
			//logger.info("Building ARP match.");
			matchRule = sw.getOFFactory().buildMatch()
				.setExact(MatchField.ETH_TYPE, EthType.ARP)
				.setExact(MatchField.ETH_SRC, srcHost.getMACAddress())
				.setExact(MatchField.ETH_DST, dstHost.getMACAddress())
				.build();
		}else if (eth.getEtherType() == EthType.IPv4) {
			//logger.info("Building IVv4 match.");
			IPv4 ipv4 = (IPv4) eth.getPayload();
			
			//Build IPv4 match
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
		OFPort portIn = OFMessageUtils.getInPort(pi);
		
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
	}
	
	
	public class CollectStats implements Runnable {
		
		protected int count = 0;
		
		public CollectStats() {
			logger.info("Starting up status thread");
		}

		@Override
		public void run() {
			//Records the link history utilization
			try {
				logger.info("Collecting bandwidth status");
				
				Map<NodePortTuple, SwitchPortBandwidth> m = statisticsService.getBandwidthConsumption();
				
				if (m != null)
				{
					for (Entry<NodePortTuple, SwitchPortBandwidth> s : m.entrySet()) {
						if(history.containsKey(s.getKey())) {		
							if (history.get(s.getKey()).remainingCapacity() <= 0 )
								history.get(s.getKey()).poll();
							
							history.get(s.getKey()).add(s.getValue().getBitsPerSecondTx());
							
						}else {
							LinkedBlockingQueue<U64> list = new LinkedBlockingQueue<U64>(HIST_SIZE);
							list.add(s.getValue().getBitsPerSecondTx());
							history.put(s.getKey(), list);
							}
						}
				}
				
				
				if( history != null) {
						String msg = new String();
						for (Entry<NodePortTuple, LinkedBlockingQueue<U64>> e : history.entrySet()) {
							msg = "Bandwidth NPT: ";
							msg = msg.concat(e.getKey().toKeyString());
							msg = msg.concat(" Traffic: ");
							for (U64 bps : e.getValue()) {
								msg = msg.concat(bps.getBigInteger().toString());
								msg = msg.concat(", ");
							}
							//logger.info(msg);					
						}
		
				}
			}
			catch (Throwable t) {
				logger.info("Unhandled exception {}", t.getMessage());
			}
			
		}

	}	

}
