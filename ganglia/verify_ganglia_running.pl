#!/usr/bin/perl -w
# -*-CPerl-*-

# verify_ganglia_running.py: Script for ganglia initialization action test.

use strict;
use LWP::UserAgent;

my $hostname       = qx(hostname -s); chomp $hostname;
my $role           = qx(/usr/share/google/get_metadata_value attributes/dataproc-role);
my $primary_master = qx(/usr/share/google/get_metadata_value attributes/dataproc-master);
my $cluster_name   = qx(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name);

if ( $hostname eq $primary_master ){
  my $hostname = 'localhost';
  my $port = '80';

  my $ua = LWP::UserAgent->new;

  my $response = $ua->get("http://${hostname}:${port}/ganglia/");

  die $response->status_line unless $response->is_success;
  my( $page_title ) = ( $response->decoded_content =~ m:<b id="page_title">([^>]+)</b>: );
  die 'Ganglia UI is not found on master node' unless( $page_title =~ /^${cluster_name}/ );
  print("Ganglia UI is running on this node.",$/);
}else{
  if ( $hostname =~ /-w-/ ){
    print("Ganglia UI should not run on worker node",$/);
  }elsif( $hostname =~ /-m-/ ){
    print("Ganglia UI should not run on additional master",$/);
  }
}
