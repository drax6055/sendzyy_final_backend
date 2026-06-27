/**
 * Test script to validate chatbot flow logic
 * Run with: node backend/test_chatbot_flow.js
 */

// Mock flow graph matching your canvas
const testFlow = {
  nodes: [
    { id: 'start_1', type: 'start', data: {} },
    { id: 'msg_1', type: 'message', data: { text: 'Welcome to iFlora Info Pvt Ltd!' } },
    { id: 'list_1', type: 'listMessage', data: { 
      text: 'Choose an option:',
      items: [
        { title: 'Pricing', description: 'View pricing' },
        { title: 'Support', description: 'Get support' },
        { title: 'Order', description: 'Place order' }
      ]
    }},
    { id: 'msg_pricing', type: 'message', data: { text: 'Plans start at $10/mo' } },
    { id: 'msg_support', type: 'message', data: { text: 'Contact support@iflora.com' } },
    { id: 'question_1', type: 'question', data: { 
      text: 'Describe your issue',
      keywords: [
        { keyword: 'billing', edgeLabel: 'Billing' },
        { keyword: 'tech', edgeLabel: 'Tech' }
      ]
    }},
    { id: 'end_1', type: 'end', data: {} }
  ],
  edges: [
    { id: 'e1', sourceNodeId: 'start_1', targetNodeId: 'msg_1', sourcePort: 'output', edgeLabel: null },
    { id: 'e2', sourceNodeId: 'msg_1', targetNodeId: 'list_1', sourcePort: 'output', edgeLabel: null },
    { id: 'e3', sourceNodeId: 'list_1', targetNodeId: 'msg_pricing', sourcePort: 'Pricing', edgeLabel: 'Pricing' },
    { id: 'e4', sourceNodeId: 'list_1', targetNodeId: 'question_1', sourcePort: 'Support', edgeLabel: 'Support' },
    { id: 'e5', sourceNodeId: 'msg_pricing', targetNodeId: 'end_1', sourcePort: 'output', edgeLabel: null },
    { id: 'e6', sourceNodeId: 'question_1', targetNodeId: 'end_1', sourcePort: 'Billing', edgeLabel: 'Billing' }
  ]
};

// Helper functions (copied from server.js logic)
function findNode(flow, nodeId) {
  return flow.nodes.find(n => n.id === nodeId);
}

function findNextNodeId(flow, sourceNodeId, portLabel) {
  const edges = flow.edges || [];
  if (!portLabel) {
    const match = edges.find(e => e.sourceNodeId === sourceNodeId);
    return match ? match.targetNodeId : null;
  }
  const label = portLabel.toLowerCase().trim();
  const match = edges.find(e =>
    e.sourceNodeId === sourceNodeId &&
    (
      (e.edgeLabel || '').toLowerCase().trim() === label ||
      (e.sourcePort || '').toLowerCase().trim() === label
    )
  );
  return match ? match.targetNodeId : null;
}

// Test cases
const tests = [
  {
    name: 'Test 1: Start → Message → List Message',
    steps: [
      { action: 'trigger', expected: 'msg_1' },
      { action: 'advance', from: 'msg_1', expected: 'list_1' }
    ]
  },
  {
    name: 'Test 2: List Message → Select "Pricing"',
    steps: [
      { action: 'match', from: 'list_1', reply: 'Pricing', expected: 'msg_pricing' }
    ]
  },
  {
    name: 'Test 3: List Message → Select "Support"',
    steps: [
      { action: 'match', from: 'list_1', reply: 'Support', expected: 'question_1' }
    ]
  },
  {
    name: 'Test 4: Question → Reply "billing"',
    steps: [
      { action: 'match', from: 'question_1', reply: 'billing', expected: 'end_1' }
    ]
  },
  {
    name: 'Test 5: Case insensitive matching',
    steps: [
      { action: 'match', from: 'list_1', reply: 'PRICING', expected: 'msg_pricing' },
      { action: 'match', from: 'question_1', reply: 'BILLING', expected: 'end_1' }
    ]
  }
];

// Run tests
console.log('🧪 Running Chatbot Flow Tests\n');

let passed = 0;
let failed = 0;

tests.forEach(test => {
  console.log(`\n📋 ${test.name}`);
  let testPassed = true;

  test.steps.forEach((step, i) => {
    if (step.action === 'trigger') {
      const startNode = findNode(testFlow, 'start_1');
      const firstNodeId = findNextNodeId(testFlow, startNode.id);
      if (firstNodeId === step.expected) {
        console.log(`  ✅ Step ${i + 1}: Trigger → ${firstNodeId}`);
      } else {
        console.log(`  ❌ Step ${i + 1}: Expected ${step.expected}, got ${firstNodeId}`);
        testPassed = false;
      }
    } else if (step.action === 'advance') {
      const nextId = findNextNodeId(testFlow, step.from);
      if (nextId === step.expected) {
        console.log(`  ✅ Step ${i + 1}: ${step.from} → ${nextId}`);
      } else {
        console.log(`  ❌ Step ${i + 1}: Expected ${step.expected}, got ${nextId}`);
        testPassed = false;
      }
    } else if (step.action === 'match') {
      const node = findNode(testFlow, step.from);
      const reply = step.reply.toLowerCase().trim();
      let matchedLabel = null;

      if (node.type === 'listMessage') {
        const items = node.data.items || [];
        for (const item of items) {
          const title = (item.title || '').toLowerCase().trim();
          if (title && reply.includes(title)) {
            matchedLabel = item.title;
            break;
          }
        }
      } else if (node.type === 'question') {
        const keywords = node.data.keywords || [];
        for (const kw of keywords) {
          const keyword = (kw.keyword || '').toLowerCase().trim();
          if (keyword && reply.includes(keyword)) {
            matchedLabel = kw.edgeLabel || kw.keyword;
            break;
          }
        }
      }

      const nextId = matchedLabel ? findNextNodeId(testFlow, step.from, matchedLabel) : null;
      if (nextId === step.expected) {
        console.log(`  ✅ Step ${i + 1}: ${step.from} + "${step.reply}" → ${nextId}`);
      } else {
        console.log(`  ❌ Step ${i + 1}: Expected ${step.expected}, got ${nextId} (matched: ${matchedLabel})`);
        testPassed = false;
      }
    }
  });

  if (testPassed) {
    console.log(`  ✅ PASSED`);
    passed++;
  } else {
    console.log(`  ❌ FAILED`);
    failed++;
  }
});

console.log(`\n\n📊 Test Results: ${passed} passed, ${failed} failed\n`);

if (failed === 0) {
  console.log('✅ All tests passed! The flow logic is working correctly.\n');
  process.exit(0);
} else {
  console.log('❌ Some tests failed. Check the logic above.\n');
  process.exit(1);
}
