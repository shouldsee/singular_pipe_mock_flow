'''
Symbolic run construct .outward_edges, .input_json and .output_json as usual 
but skip the creation of actual output files.
A symbolic node is a node with all output_files being empty
'''
import spiper
assert spiper.version_info >= (0,0,6)
from spiper.types  import Node,Flow
from spiper.types  import Path, File, Prefix
from spiper.types  import LoggedShellCommand
import random
def random_seq(self, prefix, seed = int, L = int, _output=['seq']):
	random.seed(seed)
	with open(self.output.seq,'w') as f:
		f.write('>random_sequence\n')
		buf = ''
		for i in range(L):
			buf += 'ATCG'[int(random.random()*4)]
		f.write(buf+'\n')
	return self


def transcribe(self, prefix, input = File, _output=['fasta']):
	with open(input,'r') as fi:
		with open(self.output.fasta,'w') as fo:
			fo.write(fi.read().replace('T','U'))
	return self

def mutate(self, prefix, input=File,   _seed = 0, _output=['fasta']):
	random.seed(_seed)
	with open(input,'r') as fi:
		with open(self.output.fasta,'w') as fo:
			buf = list(fi.read())
			random.shuffle(buf)
			fo.write(''.join(buf))
	return self


@Flow
def workflow(self, prefix, seed =int , L=int, 
	_output = [
	File('log'),
	]):
# 	assert 0,'REMOTE changed aaaaaa'
	print('\n[Flow running] mock=%s'%getattr(self.runner.func,'__name__','None'))
	curr  = self.runner(random_seq, prefix,  seed,  L)
	curr1 = self.config_runner(tag='const')(random_seq, prefix, 0, 100)
	curr  = self.runner(transcribe, prefix,  curr.output.seq,)
	curr  = self.runner(mutate,     prefix,  curr.output.fasta)
	#### The Flow() execution will never be skipped
	#### hence The self.output.log will always be changed
	stdout = LoggedShellCommand(['ls -lhtr',prefix.dirname()], self.output.log).rstrip()
	self.runner(copy_file, prefix+'.source.py', __file__)
	return self



@Flow
def _workflow_future(self, prefix, seed =int , L=int, 
	_output = [
	File('log'),
	]):
# 	assert 0,'REMOTE changed aaaaaa'
	print('\n[Flow running] mock=%s'%getattr(self.runner.func,'__name__','None'))
	curr  = self.runner(random_seq, prefix,  seed,  L)
	curr1 = self.config_runner(tag='const')(random_seq, prefix, 0, 100)
	curr  = self.runner(transcribe, prefix,  curr.output.seq,)
	curr  = self.runner(mutate,     prefix,  curr.output.fasta)
	stdout = LoggedShellCommand(['ls -lhtr',prefix.dirname()], self.output.log).rstrip()

	################################################
	#### The Flow() execution will never be skipped
	#### hence The self.output.log will always be changed 
	#### The only way is to construct a node in place. 
	def logged_file(self, output_file, input=Prefix, _single_file=1, _output=[]):
		LoggedShellCommand(['ls -lhtr', input.dirname(), ], output_file)
		return self
	self.runner( logged_file, self.prefix_named+'.log', self.prefix )
	self.runner(copy_file, prefix+'.source.py', __file__)
	return self



from spiper.types import Caller, DirtyKey, rgetattr
import shutil
def copy_file(self, prefix, input=File, 
	_single_file = 1, ### A single file node only tracks the file at self.prefix
	_output=[], 
	):
	'''
	#### One can also use directly move the output file, but this would break the upstream integrity 
	#### and is hence not recommended
	'''
	shutil.copy2(input, self.prefix+'.temp')
	shutil.move(self.prefix +'.temp', self.prefix)

from pprint import pprint
from spiper.types import resolve_spiper
@Flow
def backup(self, prefix, flow = Caller, _output=[]):
	# key = 'subflow..random_seq..output..seq'
	for key in [
	'subflow..random_seq..output..seq',
	'subflow..random_seq-const..output..seq',
	'subflow..transcribe..output..fasta',
	'subflow..mutate..output..fasta',
	'output..log',
	]:
		vv = resolve_spiper(flow,key)
		# pprint(vv)
		# assert 0
		self.runner(copy_file, prefix+'.' + key, resolve_spiper(flow,key))
	self.runner(copy_file, prefix+'.source.py',__file__)
	return self



# from spiper.runner import get_all_files
from spiper.graph import tree_call, get_downstream_tree, get_upstream_tree, plot_simple_graph_lr
from graphviz import Digraph
import json
def plot_graph(self, prefix, backup_result=Caller, _output=['deptree_json','deptree_dot_txt']):
	fs   = backup_result.get_all_files()
	tree = get_upstream_tree(fs, 0)
	with open( self.output.deptree_json, 'w') as f:
		json.dump(tree_call(repr,tree),f,default=repr,indent=2)
	g = plot_simple_graph_lr(fs, None, 0, 1)
	fname = g.render( self.output.deptree_dot_txt ,format='svg' )
	print('[fn]',fname)

@Flow
def run_and_backup(
	self, prefix,
	seed = int , L = int, 
	backup_prefix=File, ### we don't want to track backup_prefix
	_output = [
	# File('log'),
	]):
	
	#### execute the flow
	flow          = self.runner(workflow, prefix, seed, L)

	#### perform backup
	backup_result = self.runner(backup, backup_prefix, flow)

	#### plot a dependency graph into the backup directory
	graph_out     = self.runner(plot_graph, backup_prefix, backup_result)
	print('[workflow]done')
	return self



def main(self=None,
	prefix = None):

	from spiper.runner import cache_run, mock_run, get_changed_files, get_all_files
	from spiper.shell import LoggedShellCommand
	from spiper.types import File,CacheFile
	from pprint import pprint
	spiper.rcParams['dir_layout']='clean'
	# if prefix is None:
	prefix = Path('/tmp/spiper.symbolic/root')
	# backup_prefix = File('/home/user/.temp/backup_03_mock_flow/root')
	backup_prefix = File('~/.temp/backup_03_mock_flow/root').expand()
	prefix.dirname().rmtree_p()
	backup_prefix.dirname().rmtree_p()	

	print('\n...[start]%r'%prefix)


	#### once a workflow is defined, we can view the proposed file changes 
	fs = get_changed_files(workflow, prefix, 1, 100, verbose=0)
	pprint(fs)
	assert fs ==[
 File('/tmp/spiper.symbolic/root.workflow.log'),
 File('/tmp/spiper.symbolic/root.random_seq.seq'),
 File('/tmp/spiper.symbolic/root.random_seq_const.seq'),
 File('/tmp/spiper.symbolic/root.transcribe.fasta'),
 File('/tmp/spiper.symbolic/root.mutate.fasta'),
 File('/tmp/spiper.symbolic/root.source.py'),

 # File('/home/user/.temp/backup_03_mock_flow/root.source.py')
 ]

	### backup is conveniently defined as a workflow taking an executed workflow as an input.
	### To check the proposed backup, mock_run() the workflow first. 
	workflow_out = mock_run(workflow, prefix, 1, 100)	
	fs = get_changed_files(backup, backup_prefix, workflow_out)
	pprint(fs)
	assert fs == [
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq_const.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.transcribe.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.mutate.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.output.log'),
 # File('/tmp/spiper.symbolic/root.source.py')
 File('/home/user/.temp/backup_03_mock_flow/root.source.py')
]


	### a convenient Flow may be defined to execute the two in chain
	### If there is certain change to the workflow,
	### the backup can also be runned
	fs = get_changed_files (run_and_backup, prefix, 1, 100, backup_prefix, verbose=0)
	pprint(fs)
	assert fs == [
	File('/tmp/spiper.symbolic/root.workflow.log'),
 File('/tmp/spiper.symbolic/root.random_seq.seq'),
 File('/tmp/spiper.symbolic/root.random_seq_const.seq'),
 File('/tmp/spiper.symbolic/root.transcribe.fasta'),
 File('/tmp/spiper.symbolic/root.mutate.fasta'),
 File('/tmp/spiper.symbolic/root.source.py'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq_const.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.transcribe.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.mutate.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.output.log'),
 File('/home/user/.temp/backup_03_mock_flow/root.source.py'),

 File('/home/user/.temp/backup_03_mock_flow/root.plot_graph.deptree_json'),
 File('/home/user/.temp/backup_03_mock_flow/root.plot_graph.deptree_dot_txt'), 

 ]

	###### constants that are preserved between runs should be detected unchanged
	_  = cache_run         (run_and_backup,  prefix, 1, 100, backup_prefix, verbose=0)
	fs = get_changed_files (run_and_backup,  prefix, 2, 200, backup_prefix, verbose=0)
	pprint(fs)
	assert fs == [File('/tmp/spiper.symbolic/root.workflow.log'),
 File('/tmp/spiper.symbolic/root.random_seq.seq'),
 # File('/tmp/spiper.symbolic/root.random_seq_const.seq'),
 File('/tmp/spiper.symbolic/root.transcribe.fasta'),
 File('/tmp/spiper.symbolic/root.mutate.fasta'),
 # File('/tmp/spiper.symbolic/root.source.py'),	
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq.output.seq'),
 # File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq_const.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.transcribe.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.mutate.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.output.log'),
 # File('/home/user/.temp/backup_03_mock_flow/root.source.py'),
 File('/home/user/.temp/backup_03_mock_flow/root.plot_graph.deptree_json'),
 File('/home/user/.temp/backup_03_mock_flow/root.plot_graph.deptree_dot_txt'), 
	 ]
	##### get_all_files() return a leaf file regardless of whether is is changed
	fs = get_all_files     (run_and_backup,  prefix, 2, 200, backup_prefix, verbose=0)
	pprint(fs)
	assert fs == [
 File('/tmp/spiper.symbolic/root.workflow.log'),
 File('/tmp/spiper.symbolic/root.random_seq.seq'),
 File('/tmp/spiper.symbolic/root.random_seq_const.seq'),
 File('/tmp/spiper.symbolic/root.transcribe.fasta'),
 File('/tmp/spiper.symbolic/root.mutate.fasta'),
 File('/tmp/spiper.symbolic/root.source.py'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.random_seq_const.output.seq'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.transcribe.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.subflow.mutate.output.fasta'),
 File('/home/user/.temp/backup_03_mock_flow/root.output.log'),
 File('/home/user/.temp/backup_03_mock_flow/root.source.py'),
 File('/home/user/.temp/backup_03_mock_flow/root.plot_graph.deptree_json'),
 File('/home/user/.temp/backup_03_mock_flow/root.plot_graph.deptree_dot_txt'), 
 ]
	_  = cache_run         (run_and_backup,  prefix, 2, 200, backup_prefix, verbose=0)


if __name__ == '__main__':
	main(None,None)
