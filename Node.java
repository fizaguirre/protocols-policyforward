package net.floodlightcontroller.policyforward;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.projectfloodlight.openflow.types.DatapathId;

import javafx.scene.chart.PieChart.Data;
import net.floodlightcontroller.linkdiscovery.Link;

public class Node {
	
	protected DatapathId node;
	protected Set<Link> links;
	protected Set<DatapathId> neighbors;
	
	public Node(DatapathId n, Set<Link> l) {
		node = n;
		links = l;
		this.computeNeighborhood();
	}
	
	public DatapathId getNodeId() {
		return node;
	}
	
	public Set<Link> getLinks() {
		return links;
	}
	
	protected boolean computeNeighborhood() {
		neighbors = new HashSet<DatapathId>();
		try {
			for ( Link l : links) {
				if (l.getSrc() == node)
					neighbors.add(l.getDst());
				else if (l.getDst() == node)
					neighbors.add(l.getSrc());
			}
		}
		catch(NullPointerException e) {
			return false;
		}
		return true;
	}
	
	public Set<DatapathId> getNeighbors() {
		return neighbors;
	}
	
	public List<Long> getCostToNeighbor(DatapathId n) {
		List<Long> costs = new ArrayList<Long>();
		for ( Link l : links) {
			if (l.getSrc() == n && l.getDst() == node)
				costs.add(l.getLatency().getValue());
			else if (l.getSrc() == node && l.getDst() == n)
				costs.add(l.getLatency().getValue());
		}
		return costs;
	}

}
