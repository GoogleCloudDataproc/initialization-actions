#!/usr/bin/perl -w
# -*-CPerl-*-

# Usage: Run this script from the root directory of the git clone:
# perl templates/generate-action.pl gpu/install_gpu_driver.sh

use Template;
use strict;

my $action = $ARGV[0];
my $v = { template_path => "${action}.in" };

sub usage{ die "Usage: $0 <action>" }

usage unless( $action && -f "$ENV{PWD}/templates/$v->{template_path}" );

my $tt = Template->new( {
  INCLUDE_PATH => "$ENV{PWD}/templates",
  VARIABLES => $v,
  INTERPOLATE  => 0,
}) || die "$Template::ERROR$/";


$tt->process($v->{template_path}) or die( $tt->error(), "\n" );
