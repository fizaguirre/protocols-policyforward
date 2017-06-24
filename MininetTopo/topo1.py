"""Custom topology example

Two directly connected switches plus a host for each switch:

   host --- switch --- switch --- host

Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=mytopo' from the command line.
"""

from mininet.topo import Topo

class MyTopo( Topo ):
    "Simple topology example."

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        Topo.__init__( self )

        # Add hosts and switches
        h1 = self.addHost( 'h1' )
        h2 = self.addHost( 'h2' )
	h3 = self.addHost( 'h3' )
	h4 = self.addHost( 'h4' )
	h5 = self.addHost( 'h5' )
	h6 = self.addHost( 'h6' )


	
        sw1 = self.addSwitch( 's1' )
        sw2 = self.addSwitch( 's2' )
	sw3 = self.addSwitch( 's3' )
	

        # Add links
        self.addLink( h1 , sw1 , bw=100 )
	self.addLink( h2 , sw1 , bw=100 )
	self.addLink( h3 , sw2 , bw=100 )
	self.addLink( h4 , sw2 , bw=100 )
	self.addLink( h5 , sw3 , bw=100 )
	self.addLink( h6 , sw3 , bw=100 )
	self.addLink( sw1, sw2, bw=10 )
	self.addLink( sw1, sw3, bw=100 )
	self.addLink( sw2, sw3, bw=100 )


topos = { 'mytopo': ( lambda: MyTopo() ) }
