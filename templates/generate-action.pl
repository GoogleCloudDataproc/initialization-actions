#!/usr/bin/perl -w
# -*-CPerl-*-

# Usage: Run this script from the root directory of the git clone:
# perl templates/generate-action.pl gpu/install_gpu_driver.sh

use Template;
use strict;

# Version of Initialization Actions we will generate
my $IA_VERSION="0.1.1";

my $action = $ARGV[0];
my $v = {
  template_path => "${action}",
  IA_VERSION    => "${IA_VERSION}",
};

sub usage{
  # TODO: use File::Find to list the available actions for the user
  my $message = <<EOF;
This script evaluates a template to generate an initialization action.
The output is printed to STDOUT.

Action templates reside under templates/\${action}.in

The <action> argument is the destination action name, not the source.
EOF
  print STDERR $message;
  die "Usage:$/$0 <action>"
}

usage unless( $action && -f "$ENV{PWD}/templates/$v->{template_path}.in" );

my $tt = Template->new( {
  INCLUDE_PATH => "$ENV{PWD}/templates",
  VARIABLES    => $v,
  INTERPOLATE  => 0,
}) || die "$Template::ERROR$/";


$tt->process("$v->{template_path}.in") or die( $tt->error(), "\n" );
