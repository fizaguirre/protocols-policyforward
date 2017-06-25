package net.floodlightcontroller.policyforward;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.linkdiscovery.Link;
import net.floodlightcontroller.linkdiscovery.internal.LinkInfo;
import net.floodlightcontroller.routing.PathId;

public class Topology {
	
	protected Logger logger;
	protected Set<DatapathId> switches; //switches in the network
	protected Map<NodePortTuple, Set<Link>> topo; //Port switch connected to link
	protected Map<DatapathId, Set<Link>> topoLinks; //Links 
	
	public Topology(Map<NodePortTuple, Set<Link>> swPortLink, Map<Link, LinkInfo> links)
	{
		logger = LoggerFactory.getLogger(PolicyForward.class);
		switches = new HashSet<DatapathId>();
		topo = swPortLink;
		
		//Topology Links
		for( Map.Entry<NodePortTuple, Set<Link>> ntp : swPortLink.entrySet()) {
			topoLinks.put(ntp.getKey().getNodeId(), ntp.getValue());
		}
		
		this.initSwitcheSet(swPortLink);		
	}
	
	private void initSwitcheSet(Map<NodePortTuple, Set<Link>> swPortLink) {
		for (NodePortTuple node : this.getTopology().keySet()) {
			switches.add(node.getNodeId());
		}
	}
	
	public boolean addSwitch(DatapathId sw) {
		return switches.add(sw);
	}
	
	private DatapathId getSwitch(DatapathId sw) {
		for (DatapathId swp : switches) {
			if (swp == sw)
				return swp;
		}
		return null;
	}
	
	private Set<DatapathId> getSwitches() {
		return switches;
	}
	
	private Map<NodePortTuple, Set<Link>> getTopology()
	{
		return topo;
	}
	
	private Map<DatapathId, Set<Link>> getLinks()
	{
		return topoLinks;
	}
	
	private List<NodePortTuple> dijkstra(Map<DatapathId, Set<Link>> topo, NodePortTuple srcPort)
	{
		Map<DatapathId, U64> nodeCost = new HashMap<DatapathId, U64>();
		Set<DatapathId> unvisited = new HashSet<DatapathId>();
		List<Link> path = new LinkedList<Link>();
		
		//First step, mark the root as 0 and inf anything else
		for ( DatapathId d : topo.keySet()) {
			if ( srcPort.getNodeId() == d) {
				nodeCost.put(d, U64.ZERO);
			}
			else {
				unvisited.add(d);
				nodeCost.put(d, U64.FULL_MASK);
			}
		}
		
		
		
		
		
		return null;
	}
	
	private Link computeNeighborhoodCost(Map<DatapathId, Set<Link>> topo, Map<DatapathId, U64> costs, DatapathId node) {
		
		Link returnLink = null;
		
		for (Map.Entry<DatapathId, Set<Link>> m : topo.entrySet()) {
			if(m.getKey() == node) {
				for(Link l : m.getValue()) {
					if ( costs.get(m.getKey()).compareTo(l.getLatency()) < 0)
						returnLink = l;
				}
			}
		}
		
		return returnLink;
	}
	
	
	public void showTopology()
	{
		
		for(NodePortTuple node : this.getTopology().keySet())
		{
			String msg = new String();
			Set<Link> links = this.getTopology().get(node);
			msg = "( Node "+ node.getNodeId().toString()+",";
			for(Link link : links)
			{
				msg = msg.concat("("+link.getSrc().toString()+","+link.getSrcPort()+")");
				msg = msg.concat(" - ");
				msg = msg.concat("("+link.getDst().toString()+","+link.getDstPort()+")");
				msg = msg.concat(")");
			}
			logger.info(msg);
		}
	}
	
	public void showLinks()
	{
		String msg = new String();
		logger.info(" ---- Links ----");
		for (Map.Entry<DatapathId, Set<Link>> link : this.getLinks().entrySet())
		{
			logger.info("DatapathID: {} Links {}",link.getKey().toString(), link.getValue().toString());
		}
	}

}
