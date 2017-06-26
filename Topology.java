package net.floodlightcontroller.policyforward;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
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
	protected Map<Link, LinkInfo> links; //Links
	protected Map<NodePortTuple, Set<Link>> topo; //Port switch connected to link
	protected Map<DatapathId, Set<Link>> topoLinks; //Links
	protected Set<NodePortTuple> blockedPorts; //Blocked ports
	
	protected Set<Node> nodes; //Nodes
	
	public Topology(Map<NodePortTuple, Set<Link>> swPortLink, Map<Link, LinkInfo> l)
	{
		logger = LoggerFactory.getLogger(PolicyForward.class);
		switches = new HashSet<DatapathId>();
		topo = swPortLink;
		links = l;
		blockedPorts = new HashSet<NodePortTuple>();
		
		nodes = new HashSet<Node>();
		for ( Map.Entry<NodePortTuple, Set<Link>> m : swPortLink.entrySet() )
			nodes.add(new Node(m.getKey().getNodeId(), m.getValue()));
		
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
	
	//Review
	private Map<DatapathId, Map<DatapathId,U64>> computeNeighborhoodCost(Map<DatapathId, Set<Link>> topo, Map<DatapathId, Map<DatapathId,U64>> costs, DatapathId node) {
		U64 edgeCost;
		U64 costSum;
		for ( Link d : topo.get(node)) { //Foreach neighbor
			edgeCost = costs.get(node).get(this.getNeighbor(node, d));
			costSum = edgeCost.add(edgeCost);
			if (costSum.compareTo(edgeCost) < 0)  { //If the metric is less then the recorded
				costs.get(node).remove(this.getNeighbor(node, d)); //Remove the current entry for the map.
				costs.get(node).put(this.getNeighbor(node, d), costSum); //Add the new one
			}
		}
				
		return costs;
	}
	
	private DatapathId getNeighbor(DatapathId node, Link l) {
		if ( l.getDst() == node)
			return l.getSrc();
		return l.getDst();
	}
	
	//Return true if there is an edge betwen two nodes
	private boolean hasEdge(DatapathId node1, DatapathId node2) {
		for (Link l : this.getTopology().get(node1)) {
			if ( l.getSrc() == node1 && l.getDst() == node2)
				return true;
			else if (l.getDst() == node1 && l.getSrc() == node2)
				return true;
		}
		return false;
	}
	
	private U64 getCostFrom(DatapathId node1, DatapathId node2) {
		U64 cost = U64.FULL_MASK;
		
		for ( Link l : this.getTopology().get(node1)) {
			if (l.getSrc() == node1 && l.getDst() == node2)
				cost = l.getLatency();
			else if (l.getSrc() == node2 && l.getDst() == node1)
				cost = l.getLatency();
		}
		return cost;
	}
	
	public Set<OFPort> getSwBroadcastPorts(DatapathId sw) {
		Set<OFPort> swPorts = new HashSet<OFPort>();
		for ( Map.Entry<NodePortTuple, Set<Link>> npt : this.getTopology().entrySet()) {
			if (npt.getKey().getNodeId() == sw) {
				swPorts.add(npt.getKey().getPortId());
			}
		}
		return swPorts;
	}
	
	public void showTopology()
	{
		String msg = new String();
		try {
			for ( Node n : this.nodes) {
				msg = msg.concat("Node: "+ n.getNodeId().toString());
				for (DatapathId l : n.getNeighbors() )
					msg = msg.concat(" "+"Neighbor: "+l.toString());
				
				for (Link l : n.getLinks())
					msg = msg.concat(" " + "Link: " + l.toString());
				
				logger.info(msg);
				msg = new String();
			}
		}
		catch ( NullPointerException e ) {
			logger.info(e.toString());
		}
	}

}
