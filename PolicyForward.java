package net.floodlightcontroller.policyforward;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IListener.Command;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.*;
import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.Link;
import net.floodlightcontroller.linkdiscovery.internal.LinkInfo;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.routing.*;

public class PolicyForward extends ForwardingBase implements IOFMessageListener, IFloodlightModule, ILinkDiscoveryListener {
	
	protected static Logger logger;
	protected BlockingQueue<LDUpdate> lduUpdate; //Queue to hold pending topology updates
	protected static ILinkDiscoveryService linkDiscoveryService; //LLDP service. It handles the LLDP protocol. We need to subscribe to it to listen for LLDP topology events.
	protected IFloodlightProviderService floodlightProvider;
	protected Topology topo;

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
		
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		logger = LoggerFactory.getLogger(PolicyForward.class);
		linkDiscoveryService = context.getServiceImpl(ILinkDiscoveryService.class);
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// TODO Auto-generated method stub
		logger.info("Starting up PolicyForward module");
		linkDiscoveryService.addListener(this);
		lduUpdate = new LinkedBlockingQueue<LDUpdate>();
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			IRoutingDecision decision, FloodlightContext cntx) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		// TODO Auto-generated method stub
		switch(msg.getType())
		{
		case PACKET_IN:
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			logger.info("Packet from src mac addr {} received from switch {}", eth.getSourceMACAddress(), sw.getId().toString());
			break;
		default:
			break;
		}
		
		return Command.CONTINUE;
	}
	
	@Override
	public String getName(){
		return PolicyForward.class.getSimpleName();
	}

	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
		// TODO Auto-generated method stub
		/*
		lduUpdate.addAll(updateList); //Get a list of link layer discovery updates
		
		logger.info(" -- Received LLDP update --");
		
		LDUpdate ldu;
		while(lduUpdate.peek() != null)
		{
			try {
				ldu = lduUpdate.poll();
				logger.info("Src datapath ID {} Latency: {}", ldu.getSrc().toString(), ldu.getLatency().toString());
			}
			catch(NoSuchElementException e)
			{
				logger.info("LLDU empty");
				break;
			}
			catch(NullPointerException e)
			{
				//logger.info("{}", e.getCause());
			}
		}
		*/
		this.buildTopology();
	}
	
	protected void buildTopology()
	{
		Map<NodePortTuple, Set<Link>> npt = linkDiscoveryService.getPortLinks();
		Map<Link, LinkInfo> links = linkDiscoveryService.getLinks();
		topo = new Topology(npt, links);
		topo.showLinks();
		
		/*Map<Link, LinkInfo> links = linkDiscoveryService.getLinks();
		for (Map.Entry<Link, LinkInfo> link : links.entrySet())
			logger.info("Link {} Link Info {}", link.getKey(), link.getValue());*/
	}

}
