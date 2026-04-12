const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage();
  await p.setViewportSize({width:390, height:844});
  await p.goto('http://localhost:5001', {waitUntil:'networkidle'});
  // click Itziar story circle
  const btn = await p.$('[data-story="itziar"]');
  if (btn) await btn.click();
  await p.waitForTimeout(800);
  await p.screenshot({path:'/tmp/story_redesign2.png'});
  await b.close();
})();
