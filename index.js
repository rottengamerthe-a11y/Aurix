require('dotenv').config();

const { Client, GatewayIntentBits, ActionRowBuilder, ButtonBuilder, ButtonStyle, EmbedBuilder } = require('discord.js');
const mongoose = require('mongoose');
const express = require('express');

// ================= DATABASE =================
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log('MongoDB Connected'))
  .catch(console.error);

const userSchema = new mongoose.Schema({
  userId: String,
  aura: { type: Number, default: 1000 },
  vault: { type: Number, default: 0 },
  inventory: { type: Array, default: [] },

  skills: {
    dmg: { type: Number, default: 1 },
    luck: { type: Number, default: 1 },
    defense: { type: Number, default: 1 }
  },
  specializations: {
    dmg: { type: String, default: null },
    defense: { type: String, default: null },
    luck: { type: String, default: null }
  },

  wins: { type: Number, default: 0 },
  losses: { type: Number, default: 0 },

  rank: { type: String, default: 'Bronze' },
  clan: { type: String, default: null },
  clanRole: { type: String, default: null },
  clanInvites: { type: Array, default: [] },
  clanXp: { type: Number, default: 0 },
  clanLevel: { type: Number, default: 1 },

  xp: { type: Number, default: 0 },
  level: { type: Number, default: 1 },

  lastDaily: { type: Number, default: 0 },
  streak: { type: Number, default: 0 },
  lastVaultInterest: { type: Number, default: 0 },

  cooldowns: { type: Object, default: {} },
  activeBoosts: { type: Object, default: {} }
});

const User = mongoose.model('User', userSchema);

async function getUser(id) {
  let user = await User.findOne({ userId: id });
  if (!user) {
    user = await User.create({ userId: id });
  }

  if (!Number.isInteger(user.level) || user.level < 1) user.level = 1;
  if (!Number.isInteger(user.xp) || user.xp < 0) user.xp = 0;
  if (!Number.isInteger(user.clanLevel) || user.clanLevel < 1) user.clanLevel = 1;
  if (!Number.isInteger(user.clanXp) || user.clanXp < 0) user.clanXp = 0;
  if (!user.specializations || typeof user.specializations !== 'object') {
    user.specializations = { dmg: null, defense: null, luck: null };
  }
  for (const branch of ['dmg', 'defense', 'luck']) {
    if (!(branch in user.specializations)) user.specializations[branch] = null;
  }
  if (!Array.isArray(user.clanInvites)) user.clanInvites = [];
  if (user.clanRole !== 'owner' && user.clanRole !== 'member') user.clanRole = user.clan ? 'member' : null;
  if (!Number.isInteger(user.lastVaultInterest) || user.lastVaultInterest < 0) {
    user.lastVaultInterest = Date.now();
  }

  return user;
}

// ================= CLIENT =================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
});

let battles = new Map();
let pendingChallenges = new Map();
let pendingClanWars = new Map();
let activeClanWars = new Map();
let activeBossBattles = new Map();
let activeClanBossRaids = new Map();

const VAULT_INTEREST_RATE = 0.02;
const VAULT_INTEREST_INTERVAL = 24 * 60 * 60 * 1000;
const MAX_INTEREST_PERIODS = 7;

const CLAN_CREATE_COST = 30000;

const RANKS = [
  { name: 'Bronze', min: 0, perks: { auraBonus: 0, xpBonus: 0, dailyBonus: 0, vaultBonus: 0 }, reward: 'No extra perks yet.' },
  { name: 'Silver', min: 100000, perks: { auraBonus: 0.03, xpBonus: 0.03, dailyBonus: 0.05, vaultBonus: 0.0025 }, reward: '+3% Aura, +3% XP, +5% Daily, +0.25% vault interest.' },
  { name: 'Gold', min: 300000, perks: { auraBonus: 0.06, xpBonus: 0.06, dailyBonus: 0.1, vaultBonus: 0.005 }, reward: '+6% Aura, +6% XP, +10% Daily, +0.5% vault interest.' },
  { name: 'Platinum', min: 750000, perks: { auraBonus: 0.1, xpBonus: 0.1, dailyBonus: 0.15, vaultBonus: 0.0075 }, reward: '+10% Aura, +10% XP, +15% Daily, +0.75% vault interest.' },
  { name: 'Diamond', min: 1500000, perks: { auraBonus: 0.15, xpBonus: 0.15, dailyBonus: 0.2, vaultBonus: 0.01 }, reward: '+15% Aura, +15% XP, +20% Daily, +1% vault interest.' },
  { name: 'Master', min: 3000000, perks: { auraBonus: 0.2, xpBonus: 0.2, dailyBonus: 0.3, vaultBonus: 0.0125 }, reward: '+20% Aura, +20% XP, +30% Daily, +1.25% vault interest.' },
  { name: 'Legend', min: 6000000, perks: { auraBonus: 0.3, xpBonus: 0.3, dailyBonus: 0.4, vaultBonus: 0.015 }, reward: '+30% Aura, +30% XP, +40% Daily, +1.5% vault interest.' }
];

const CLAN_LEVELS = [
  { level: 1, reward: 'Clan unlocked. Member cap 10.', perks: { memberCap: 10, auraBonus: 0, xpBonus: 0, dailyBonus: 0, vaultBonus: 0 } },
  { level: 2, reward: '+2% clan Aura gain.', perks: { memberCap: 10, auraBonus: 0.02, xpBonus: 0, dailyBonus: 0, vaultBonus: 0 } },
  { level: 3, reward: '+5% clan XP gain.', perks: { memberCap: 10, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0, vaultBonus: 0 } },
  { level: 4, reward: '+0.25% clan vault interest.', perks: { memberCap: 10, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0, vaultBonus: 0.0025 } },
  { level: 5, reward: '+5% clan daily reward.', perks: { memberCap: 10, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 6, reward: 'Member cap increased to 12.', perks: { memberCap: 12, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 7, reward: '+4% total clan Aura gain.', perks: { memberCap: 12, auraBonus: 0.04, xpBonus: 0.05, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 8, reward: '+10% total clan XP gain.', perks: { memberCap: 12, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 9, reward: '+0.5% total clan vault interest.', perks: { memberCap: 12, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.05, vaultBonus: 0.005 } },
  { level: 10, reward: '+10% total clan daily reward.', perks: { memberCap: 12, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 11, reward: 'Member cap increased to 14.', perks: { memberCap: 14, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 12, reward: '+6% total clan Aura gain.', perks: { memberCap: 14, auraBonus: 0.06, xpBonus: 0.1, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 13, reward: '+15% total clan XP gain.', perks: { memberCap: 14, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 14, reward: '+0.75% total clan vault interest.', perks: { memberCap: 14, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.1, vaultBonus: 0.0075 } },
  { level: 15, reward: '+15% total clan daily reward.', perks: { memberCap: 14, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 16, reward: 'Member cap increased to 16.', perks: { memberCap: 16, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 17, reward: '+8% total clan Aura gain.', perks: { memberCap: 16, auraBonus: 0.08, xpBonus: 0.15, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 18, reward: '+20% total clan XP gain.', perks: { memberCap: 16, auraBonus: 0.08, xpBonus: 0.2, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 19, reward: '+1% total clan vault interest.', perks: { memberCap: 16, auraBonus: 0.08, xpBonus: 0.2, dailyBonus: 0.15, vaultBonus: 0.01 } },
  { level: 20, reward: 'Final clan rank: member cap 20, +10% Aura, +25% XP, +20% Daily, +1.25% vault interest.', perks: { memberCap: 20, auraBonus: 0.1, xpBonus: 0.25, dailyBonus: 0.2, vaultBonus: 0.0125 } }
];

const BOOST_LABELS = {
  aura: 'Aura Boost',
  daily: 'Daily Boost',
  crate: 'Luck Boost',
  xp: 'XP Boost'
};

const SHOP_ITEMS = {
  crate: {
    name: 'Crate',
    price: 2000,
    maxOwned: 25,
    description: 'Open it with !open for a random Aura reward.'
  },
  auraboost: {
    name: 'Aura Boost',
    price: 15000,
    boostKey: 'aura',
    multiplier: 2,
    durationMs: 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use auraboost to double Aura rewards for 1 hour.'
  },
  dailyboost: {
    name: 'Daily Boost',
    price: 20000,
    boostKey: 'daily',
    multiplier: 2,
    durationMs: 24 * 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use dailyboost to double daily rewards for 24 hours.'
  },
  luckboost: {
    name: 'Luck Boost',
    price: 12000,
    boostKey: 'crate',
    multiplier: 1.5,
    durationMs: 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use luckboost to increase crate rewards by 50% for 1 hour.'
  },
  xpboost: {
    name: 'XP Boost',
    price: 18000,
    boostKey: 'xp',
    multiplier: 2,
    durationMs: 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use xpboost to double XP rewards for 1 hour.'
  }
};

const SKILL_SPECIALIZATIONS = {
  dmg: {
    berserker: 'Increase attack damage by 20%.',
    assassin: 'Increase crit chance by 12%.'
  },
  defense: {
    guardian: 'Increase max HP and make Defend stronger.',
    medic: 'Increase healing by 25%.'
  },
  luck: {
    fortune: 'Increase crit chance by 5% and reward gains by 5%.',
    trickster: 'Gain 10% dodge chance against enemy attacks.'
  }
};

const BOSS_TEMPLATES = [
  {
    key: 'ember',
    name: 'Ember Titan',
    description: 'A fire giant with heavy Aura rewards.',
    minLevel: 3,
    hp: 18000,
    clanHp: 70000,
    attack: 1400,
    defense: 120,
    aura: 18000,
    xp: 220,
    clanXp: 80,
    perk: { key: 'aura', multiplier: 1.5, durationMs: 60 * 60 * 1000, label: 'Titan Aura Surge' }
  },
  {
    key: 'warden',
    name: 'Vault Warden',
    description: 'A durable boss that rewards growth and economy perks.',
    minLevel: 6,
    hp: 26000,
    clanHp: 95000,
    attack: 1800,
    defense: 180,
    aura: 26000,
    xp: 320,
    clanXp: 120,
    perk: { key: 'daily', multiplier: 1.5, durationMs: 24 * 60 * 60 * 1000, label: 'Warden Daily Blessing' }
  },
  {
    key: 'oracle',
    name: 'Void Oracle',
    description: 'A luck-based raid boss with strong XP rewards.',
    minLevel: 10,
    hp: 36000,
    clanHp: 130000,
    attack: 2400,
    defense: 240,
    aura: 36000,
    xp: 460,
    clanXp: 180,
    perk: { key: 'xp', multiplier: 1.5, durationMs: 90 * 60 * 1000, label: 'Oracle Insight' }
  }
];

const COMMAND_INFO = [
  { name: '!help', description: 'Shows all available commands and what they do.' },
  { name: '!spin', description: 'Spin the slot machine for Aura and XP with a cooldown.' },
  { name: '!coinflip <heads/tails> <amount>', description: 'Flip a coin and win Aura and XP if your guess is correct.' },
  { name: '!pvp @user', description: 'Challenge another player to a PvP fight.' },
  { name: '!skills', description: 'Shows your skill tree, points, and upgrade costs.' },
  { name: '!skill upgrade <dmg|defense|luck>', description: 'Spend a skill point to upgrade one branch.' },
  { name: '!skill paths', description: 'Shows available skill specializations.' },
  { name: '!skill specialize <dmg|defense|luck> <path>', description: 'Choose a specialization after reaching skill level 5 in that branch.' },
  { name: '!bal', description: 'Shows your wallet balance, vault balance, and current rank.' },
  { name: '!rank', description: 'Shows your current rank, progress, and rank perks.' },
  { name: '!level', description: 'Shows your level, XP, and progress to the next level.' },
  { name: '!deposit <amount|all>', description: 'Moves Aura from your wallet into your vault.' },
  { name: '!daily', description: 'Claims your daily Aura and XP reward.' },
  { name: '!vaultinterest', description: 'Shows your vault interest details and current daily rate.' },
  { name: '!shop', description: 'Displays the shop and available items to buy.' },
  { name: '!buy <item>', description: 'Buys an item from the shop if you have enough Aura.' },
  { name: '!use <item>', description: 'Uses a boost item from your inventory.' },
  { name: '!open', description: 'Opens a crate from your inventory for Aura and XP.' },
  { name: '!clan create <name>', description: 'Creates a new clan for 30000 Aura if the name is not taken.' },
  { name: '!clan invite @user', description: 'Clan owner invites a player to the clan.' },
  { name: '!clan join <name>', description: 'Joins an existing clan if you have an invite.' },
  { name: '!clan accept <name>', description: 'Accepts a pending clan invite.' },
  { name: '!clan decline <name>', description: 'Declines a pending clan invite.' },
  { name: '!clan leave', description: 'Leaves your current clan.' },
  { name: '!clan transfer @user', description: 'Transfers clan ownership to another clan member.' },
  { name: '!clan kick @user', description: 'Clan owner removes a member from the clan.' },
  { name: '!clan war <name>', description: 'Clan owner challenges another clan to a clan war.' },
  { name: '!clan war join', description: 'Join your clan roster for an active clan war.' },
  { name: '!clan war leave', description: 'Leave your clan war roster before the war starts.' },
  { name: '!clan war start', description: 'Clan owner starts the active clan war once rosters are ready.' },
  { name: '!clan war status', description: 'Shows the current active clan war, rosters, and logs.' },
  { name: '!boss list', description: 'Shows all bosses, their levels, rewards, and perks.' },
  { name: '!boss start <boss>', description: 'Start a solo boss fight against a named boss.' },
  { name: '!boss attack', description: 'Attack your current solo boss.' },
  { name: '!boss heal', description: 'Heal during your current solo boss fight.' },
  { name: '!boss status', description: 'Shows your current solo boss battle status.' },
  { name: '!boss clan start <boss>', description: 'Clan owner starts a clan boss raid.' },
  { name: '!boss clan join', description: 'Join your clan boss raid roster.' },
  { name: '!boss clan attack', description: 'Attack the active clan boss.' },
  { name: '!boss clan heal', description: 'Heal yourself during a clan boss raid.' },
  { name: '!boss clan status', description: 'Shows the current clan boss raid status.' },
  { name: '!clan info [name]', description: 'Shows member and stats info for a clan.' },
  { name: '!clan top', description: 'Shows the strongest clans by total Aura.' },
  { name: '!leaderboard', description: 'Shows the top players by Aura.' },
  { name: '!inv', description: 'Displays your inventory and active boosts.' },
  { name: '!stats', description: 'Shows your rank, level, wins, losses, and boosts.' }
];

// ================= UTILS =================
const EMBED_COLORS = {
  primary: 0xf59e0b,
  success: 0x22c55e,
  danger: 0xef4444,
  info: 0x38bdf8,
  royal: 0x8b5cf6
};

function createEmbed(message, title, color = EMBED_COLORS.primary) {
  return new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setFooter({ text: `${message.author.username} • Aura Realms` })
    .setTimestamp();
}

function field(name, value, inline = true) {
  return { name, value: String(value), inline };
}

function combatEmbed(title, color, description, fields = []) {
  const embed = new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setDescription(description)
    .setTimestamp();

  if (fields.length > 0) {
    embed.addFields(fields);
  }

  return embed;
}

function warningEmbed(message, title, description) {
  return createEmbed(message, title, EMBED_COLORS.danger).setDescription(description);
}

function infoEmbed(message, title, description) {
  return createEmbed(message, title, EMBED_COLORS.info).setDescription(description);
}

function interactionNoticeEmbed(title, description, color = EMBED_COLORS.info) {
  return new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setDescription(description)
    .setTimestamp();
}

function cooldown(user, key, time) {
  if (!user.cooldowns || typeof user.cooldowns !== 'object') {
    user.cooldowns = {};
  }

  const expiresAt = user.cooldowns[key] || 0;
  if (Date.now() < expiresAt) {
    return Math.ceil((expiresAt - Date.now()) / 1000);
  }

  user.cooldowns[key] = Date.now() + time;
  user.markModified('cooldowns');
  return 0;
}

function bar(hp, max) {
  const filled = Math.round((hp / max) * 10);
  return '#'.repeat(filled) + '-'.repeat(10 - filled);
}

function xpNeededForLevel(level) {
  return 500 + (level - 1) * 250;
}

function getRankData(user) {
  const totalAura = user.aura + user.vault;
  let currentRank = RANKS[0];
  let nextRank = null;

  for (let i = 0; i < RANKS.length; i++) {
    if (totalAura >= RANKS[i].min) {
      currentRank = RANKS[i];
      nextRank = RANKS[i + 1] || null;
    } else {
      break;
    }
  }

  return { totalAura, currentRank, nextRank };
}

function getRankPerks(user) {
  return getRankData(user).currentRank.perks;
}

function getRankRewardText(user) {
  return getRankData(user).currentRank.reward;
}

function clanXpNeededForLevel(level) {
  return 1500 + (level - 1) * 1000;
}

function getClanLevelData(level) {
  return CLAN_LEVELS[Math.min(Math.max(level, 1), CLAN_LEVELS.length) - 1];
}

function getClanPerks(user) {
  return user.clan ? getClanLevelData(user.clanLevel || 1).perks : {
    memberCap: 0,
    auraBonus: 0,
    xpBonus: 0,
    dailyBonus: 0,
    vaultBonus: 0
  };
}

function updateRank(user) {
  const { currentRank } = getRankData(user);
  user.rank = currentRank.name;
}

function rankProgress(user) {
  const { totalAura, currentRank, nextRank } = getRankData(user);

  if (!nextRank) {
    return `Rank: ${currentRank.name} | Total Aura: ${totalAura} | Max rank reached`;
  }

  const progress = totalAura - currentRank.min;
  const needed = nextRank.min - currentRank.min;
  const remaining = nextRank.min - totalAura;

  return `Rank: ${currentRank.name} | Progress: ${progress}/${needed} | ${remaining} Aura to ${nextRank.name}`;
}

function levelProgress(user) {
  const needed = xpNeededForLevel(user.level);
  return `Level: ${user.level} | XP: ${user.xp}/${needed} | ${Math.max(needed - user.xp, 0)} XP to level ${user.level + 1}`;
}

function spentSkillPoints(user) {
  return (user.skills.dmg - 1) + (user.skills.defense - 1) + (user.skills.luck - 1);
}

function availableSkillPoints(user) {
  return Math.max(user.level - 1 - spentSkillPoints(user), 0);
}

function nextSkillCost(user, skillName) {
  return user.skills[skillName];
}

function getSpecialization(user, branch) {
  return user.specializations?.[branch] || null;
}

function skillTreeSummary(user) {
  return `Skill Points: ${availableSkillPoints(user)} | Damage: ${user.skills.dmg} (${getSpecialization(user, 'dmg') || 'no path'}, next cost ${nextSkillCost(user, 'dmg')}) | Defense: ${user.skills.defense} (${getSpecialization(user, 'defense') || 'no path'}, next cost ${nextSkillCost(user, 'defense')}) | Luck: ${user.skills.luck} (${getSpecialization(user, 'luck') || 'no path'}, next cost ${nextSkillCost(user, 'luck')})`;
}

function specializationSummary() {
  return Object.entries(SKILL_SPECIALIZATIONS)
    .map(([branch, paths]) => `${branch}: ${Object.entries(paths).map(([name, text]) => `${name} - ${text}`).join(' | ')}`)
    .join('\n');
}

function clanLevelProgress(user) {
  const data = getClanLevelData(user.clanLevel || 1);
  if ((user.clanLevel || 1) >= CLAN_LEVELS.length) {
    return `Clan Level: ${data.level}/20 | Clan XP: ${user.clanXp || 0} | Max clan level reached | Reward: ${data.reward}`;
  }

  const needed = clanXpNeededForLevel(user.clanLevel || 1);
  return `Clan Level: ${data.level}/20 | Clan XP: ${user.clanXp || 0}/${needed} | ${Math.max(needed - (user.clanXp || 0), 0)} Clan XP to level ${(user.clanLevel || 1) + 1} | Reward: ${data.reward}`;
}

function ensureBoosts(user) {
  if (!user.activeBoosts || typeof user.activeBoosts !== 'object') {
    user.activeBoosts = {};
    user.markModified('activeBoosts');
  }
}

function removeExpiredBoosts(user) {
  ensureBoosts(user);

  let changed = false;
  for (const [key, boost] of Object.entries(user.activeBoosts)) {
    if (!boost || Date.now() >= boost.expiresAt) {
      delete user.activeBoosts[key];
      changed = true;
    }
  }

  if (changed) {
    user.markModified('activeBoosts');
  }
}

function hasItem(user, itemName) {
  return user.inventory.includes(itemName);
}

function countItem(user, itemName) {
  return user.inventory.filter(item => item === itemName).length;
}

function removeItem(user, itemName) {
  const itemIndex = user.inventory.indexOf(itemName);
  if (itemIndex === -1) return false;

  user.inventory.splice(itemIndex, 1);
  return true;
}

function isBoostActive(user, key) {
  removeExpiredBoosts(user);
  return Boolean(user.activeBoosts[key]);
}

function activateBoost(user, key, multiplier, durationMs) {
  ensureBoosts(user);
  removeExpiredBoosts(user);

  if (user.activeBoosts[key]) {
    return false;
  }

  user.activeBoosts[key] = {
    multiplier,
    expiresAt: Date.now() + durationMs
  };

  user.markModified('activeBoosts');
  return true;
}

function getBoostMultiplier(user, key) {
  removeExpiredBoosts(user);
  const boost = user.activeBoosts[key];
  return boost ? (boost.multiplier || 1) : 1;
}

function addAura(user, baseAmount, boostKey = 'aura') {
  const multiplier = getBoostMultiplier(user, boostKey);
  const rankPerks = getRankPerks(user);
  const clanPerks = getClanPerks(user);
  const bonus = rankPerks.auraBonus + clanPerks.auraBonus + (boostKey === 'daily' ? rankPerks.dailyBonus + clanPerks.dailyBonus : 0);
  const reward = Math.floor(baseAmount * multiplier * (1 + bonus));

  user.aura += reward;
  updateRank(user);

  return { reward, multiplier, passiveBonus: bonus };
}

function addXp(user, baseAmount) {
  const multiplier = getBoostMultiplier(user, 'xp');
  const rankPerks = getRankPerks(user);
  const clanPerks = getClanPerks(user);
  const reward = Math.floor(baseAmount * multiplier * (1 + rankPerks.xpBonus + clanPerks.xpBonus));

  user.xp += reward;

  let levelsGained = 0;
  while (user.xp >= xpNeededForLevel(user.level)) {
    user.xp -= xpNeededForLevel(user.level);
    user.level += 1;
    levelsGained += 1;
  }

  return { reward, multiplier, levelsGained };
}

function applyVaultInterest(user) {
  const now = Date.now();

  if (!user.vault || user.vault <= 0) {
    user.lastVaultInterest = now;
    return { applied: 0, periods: 0 };
  }

  if (!user.lastVaultInterest) {
    user.lastVaultInterest = now;
    return { applied: 0, periods: 0 };
  }

  const elapsed = now - user.lastVaultInterest;
  const periods = Math.min(Math.floor(elapsed / VAULT_INTEREST_INTERVAL), MAX_INTEREST_PERIODS);
  const rankPerks = getRankPerks(user);
  const clanPerks = getClanPerks(user);
  const effectiveRate = VAULT_INTEREST_RATE + rankPerks.vaultBonus + clanPerks.vaultBonus;

  if (periods <= 0) {
    return { applied: 0, periods: 0 };
  }

  let totalInterest = 0;
  for (let i = 0; i < periods; i++) {
    const interest = Math.floor(user.vault * effectiveRate);
    if (interest <= 0) break;
    user.vault += interest;
    totalInterest += interest;
  }

  user.lastVaultInterest += periods * VAULT_INTEREST_INTERVAL;
  updateRank(user);

  return { applied: totalInterest, periods, rate: effectiveRate };
}

function formatDuration(ms) {
  const totalSeconds = Math.ceil(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);

  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function inventorySummary(user) {
  if (user.inventory.length === 0) return 'Inventory empty';

  const counts = {};
  for (const item of user.inventory) {
    counts[item] = (counts[item] || 0) + 1;
  }

  return Object.entries(counts)
    .map(([item, count]) => `${item} x${count}`)
    .join(', ');
}

function activeBoostSummary(user) {
  removeExpiredBoosts(user);

  const active = Object.entries(user.activeBoosts)
    .map(([key, boost]) => `${BOOST_LABELS[key] || key} x${boost.multiplier} (${formatDuration(boost.expiresAt - Date.now())} left)`);

  return active.length > 0 ? active.join(', ') : 'No active boosts';
}

function formatProgressExtras(xpResult) {
  const parts = [];

  if (xpResult.reward > 0) {
    parts.push(`+${xpResult.reward} XP${xpResult.multiplier > 1 ? ` (${xpResult.multiplier}x XP boost)` : ''}`);
  }

  if (xpResult.levelsGained > 0) {
    parts.push(`Level up! You are now level ${xpResult.level}`);
  }

  return parts.length > 0 ? ` | ${parts.join(' | ')}` : '';
}

function formatClanProgressExtras(clanXpResult) {
  if (!clanXpResult || clanXpResult.gained <= 0) return '';

  const parts = [`+${clanXpResult.gained} Clan XP`];
  if (clanXpResult.levelUps > 0) {
    parts.push(`Clan level up! Now level ${clanXpResult.level}`);
  }

  return ` | ${parts.join(' | ')}`;
}

function buildBattleRow(disabled = false) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('attack')
      .setLabel('Attack')
      .setStyle(ButtonStyle.Danger)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId('heal')
      .setLabel('Heal')
      .setStyle(ButtonStyle.Success)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId('defend')
      .setLabel('Defend')
      .setStyle(ButtonStyle.Primary)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId('critical')
      .setLabel('Critical')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(disabled)
  );
}

function battleMaxHp(user) {
  const guardianBonus = getSpecialization(user, 'defense') === 'guardian' ? 1500 : 0;
  return 10000 + user.skills.defense * 250 + guardianBonus;
}

function resolveBattleDamage(attacker, defender, battle, mode = 'attack') {
  const attackerId = attacker.userId;
  const defenderId = defender.userId;

  let baseDamage = mode === 'critical'
    ? 1800 + attacker.skills.dmg * 320
    : 1500 + attacker.skills.dmg * 250;

  if (getSpecialization(attacker, 'dmg') === 'berserker') {
    baseDamage *= 1.2;
  }

  let critChance = mode === 'critical' ? 0.55 : 0.15;
  if (getSpecialization(attacker, 'dmg') === 'assassin') critChance += 0.12;
  if (getSpecialization(attacker, 'luck') === 'fortune') critChance += 0.05;

  const dodgeChance = getSpecialization(defender, 'luck') === 'trickster' ? 0.1 : 0;
  if (Math.random() < dodgeChance) {
    battle.defending[defenderId] = false;
    return { damage: 0, crit: false, dodged: true };
  }

  let damage = Math.floor(baseDamage);
  damage = Math.max(250, damage - defender.skills.defense * 50);

  if (battle.defending[defenderId]) {
    const defendMultiplier = getSpecialization(defender, 'defense') === 'guardian' ? 0.35 : 0.5;
    damage = Math.floor(damage * defendMultiplier);
    battle.defending[defenderId] = false;
  }

  const crit = Math.random() < critChance;
  if (crit) damage *= 2;

  battle.hp[defenderId] -= damage;
  return { damage, crit, dodged: false };
}

async function syncClanProgress(clanName, clanLevel, clanXp) {
  await User.updateMany(
    { clan: clanName },
    { $set: { clanLevel, clanXp } }
  );
}

function normalizeClanName(name) {
  return name.trim().replace(/\s+/g, ' ');
}

function isValidClanName(name) {
  return /^[a-zA-Z0-9 ]{3,20}$/.test(name);
}

async function findClanMembers(clanName) {
  return User.find({ clan: clanName }).sort({ aura: -1 });
}

async function getClanOwner(clanName) {
  return User.findOne({ clan: clanName, clanRole: 'owner' });
}

async function getClanSummary(clanName) {
  const members = await findClanMembers(clanName);
  if (members.length === 0) return null;

  const totalAura = members.reduce((sum, member) => sum + member.aura + member.vault, 0);
  const totalWins = members.reduce((sum, member) => sum + member.wins, 0);

  return {
    name: clanName,
    members,
    totalAura,
    totalWins
  };
}

function getClanPower(summary) {
  return summary.members.reduce((total, member) => {
    const base = member.aura + member.vault;
    const combat = member.level * 500 + member.wins * 250 + member.skills.dmg * 200 + member.skills.defense * 200 + member.skills.luck * 150;
    return total + base + combat;
  }, 0);
}

function clanWarParticipantCap(level) {
  return Math.min(3 + Math.floor((level - 1) / 5), 5);
}

function clanBossParticipantCap(level) {
  return Math.min(4 + Math.floor((level - 1) / 4), 8);
}

function getActiveClanWar(clanName) {
  for (const war of activeClanWars.values()) {
    if (war.attackerClan === clanName || war.defenderClan === clanName) {
      return war;
    }
  }
  return null;
}

function memberWarPower(member) {
  const base = member.level * 500 + member.wins * 250;
  const skills = member.skills.dmg * 220 + member.skills.defense * 220 + member.skills.luck * 160;
  const resources = Math.floor((member.aura + member.vault) * 0.02);
  return base + skills + resources;
}

async function resolveClanWar(activeWar) {
  const attackerRoster = await User.find({ userId: { $in: activeWar.attackerParticipants } });
  const defenderRoster = await User.find({ userId: { $in: activeWar.defenderParticipants } });

  let attackerRounds = 0;
  let defenderRounds = 0;

  const rounds = Math.min(3, Math.max(attackerRoster.length, defenderRoster.length));
  for (let round = 0; round < rounds; round++) {
    const attacker = attackerRoster[round % attackerRoster.length];
    const defender = defenderRoster[round % defenderRoster.length];

    const attackerPower = memberWarPower(attacker) * (0.9 + Math.random() * 0.2);
    const defenderPower = memberWarPower(defender) * (0.9 + Math.random() * 0.2);

    if (attackerPower >= defenderPower) {
      attackerRounds += 1;
      activeWar.logs.push(`Round ${round + 1}: ${attacker.userId} carried ${activeWar.attackerClan} over ${defender.userId}.`);
    } else {
      defenderRounds += 1;
      activeWar.logs.push(`Round ${round + 1}: ${defender.userId} carried ${activeWar.defenderClan} over ${attacker.userId}.`);
    }
  }

  const winnerClan = attackerRounds >= defenderRounds ? activeWar.attackerClan : activeWar.defenderClan;
  const loserClan = winnerClan === activeWar.attackerClan ? activeWar.defenderClan : activeWar.attackerClan;
  const winnerClanXp = await addClanXp(winnerClan, 400);
  await User.updateMany({ clan: winnerClan }, { $inc: { aura: 6000 } });
  await User.updateMany({ clan: loserClan }, { $inc: { aura: 2000 } });

  activeWar.logs.push(`Final Result: ${winnerClan} won ${Math.max(attackerRounds, defenderRounds)}-${Math.min(attackerRounds, defenderRounds)}.`);

  return { winnerClan, loserClan, attackerRounds, defenderRounds, winnerClanXp };
}

async function addClanXp(clanName, baseAmount) {
  if (!clanName || baseAmount <= 0) {
    return { gained: 0, levelUps: 0, level: 1, xp: 0 };
  }

  const members = await findClanMembers(clanName);
  if (members.length === 0) {
    return { gained: 0, levelUps: 0, level: 1, xp: 0 };
  }

  const reference = members[0];
  let clanLevel = reference.clanLevel || 1;
  let clanXp = reference.clanXp || 0;
  const gained = Math.floor(baseAmount);

  clanXp += gained;

  let levelUps = 0;
  while (clanLevel < CLAN_LEVELS.length && clanXp >= clanXpNeededForLevel(clanLevel)) {
    clanXp -= clanXpNeededForLevel(clanLevel);
    clanLevel += 1;
    levelUps += 1;
  }

  await syncClanProgress(clanName, clanLevel, clanXp);
  return { gained, levelUps, level: clanLevel, xp: clanXp };
}

function findBoss(query) {
  if (!query) return null;
  const normalized = query.trim().toLowerCase();
  return BOSS_TEMPLATES.find(boss => boss.key === normalized || boss.name.toLowerCase() === normalized);
}

function bossListText() {
  return BOSS_TEMPLATES
    .map(boss => `- ${boss.name} (${boss.key}): Lv ${boss.minLevel} | Solo HP ${boss.hp} | Clan HP ${boss.clanHp} | Rewards ${boss.aura} Aura, ${boss.xp} XP, ${boss.clanXp} Clan XP | Perk ${boss.perk.label}`)
    .join('\n');
}

function bossPlayerAttack(user, boss) {
  let baseDamage = 1700 + user.skills.dmg * 260 + user.level * 35;

  if (getSpecialization(user, 'dmg') === 'berserker') {
    baseDamage *= 1.2;
  }

  let critChance = 0.16;
  if (getSpecialization(user, 'dmg') === 'assassin') critChance += 0.12;
  if (getSpecialization(user, 'luck') === 'fortune') critChance += 0.05;

  const crit = Math.random() < critChance;
  let damage = Math.max(400, Math.floor(baseDamage) - boss.defense);
  if (crit) damage *= 2;

  return { damage, crit };
}

function bossCounterDamage(user, boss) {
  let damage = boss.attack - user.skills.defense * 60 - user.level * 20;
  if (getSpecialization(user, 'defense') === 'guardian') {
    damage *= 0.85;
  }

  if (getSpecialization(user, 'luck') === 'trickster' && Math.random() < 0.1) {
    return { damage: 0, dodged: true };
  }

  return { damage: Math.max(250, Math.floor(damage)), dodged: false };
}

function bossHealAmount(user, missingHp) {
  let amount = 1400 + user.skills.defense * 220 + user.skills.luck * 60;
  if (getSpecialization(user, 'defense') === 'medic') {
    amount *= 1.25;
  }
  return Math.min(missingHp, Math.floor(amount));
}

function soloBossStatusText(state) {
  return `Boss: ${state.boss.name}\nBoss HP: ${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}\nYour HP: ${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}\nHeals left: ${state.healsLeft}\nReward on win: ${state.boss.aura} Aura, ${state.boss.xp} XP, ${state.boss.clanXp} Clan XP, perk ${state.boss.perk.label}`;
}

function getActiveClanBossRaid(clanName) {
  return clanName ? activeClanBossRaids.get(clanName) || null : null;
}

function clanBossStatusText(raid) {
  const roster = raid.participants.length > 0
    ? raid.participants.map(id => {
        const hp = raid.playerHp[id] || 0;
        const maxHp = raid.playerMaxHp[id] || 1;
        return `<@${id}> ${Math.max(hp, 0)}/${maxHp}`;
      }).join('\n')
    : 'No participants yet.';

  const recentLogs = raid.logs.slice(-5).join('\n') || 'No logs yet.';

  return `Clan Boss: ${raid.boss.name}\nClan: ${raid.clanName}\nBoss HP: ${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}\nParticipants: ${raid.participants.length}\nRoster:\n${roster}\nLogs:\n${recentLogs}`;
}

async function rewardClanBossRaid(raid) {
  const participantUsers = await User.find({ userId: { $in: raid.participants } });
  const damageEntries = Object.entries(raid.damageByUser || {});
  const topDamagerId = damageEntries.sort((a, b) => b[1] - a[1])[0]?.[0] || null;
  const rewardLines = [];

  for (const member of participantUsers) {
    const auraResult = addAura(member, raid.boss.aura, 'aura');
    const xpResult = addXp(member, raid.boss.xp);
    xpResult.level = member.level;

    let perkText = '';
    if (member.userId === topDamagerId) {
      const granted = activateBoost(member, raid.boss.perk.key, raid.boss.perk.multiplier, raid.boss.perk.durationMs);
      perkText = granted ? ` | Perk: ${raid.boss.perk.label}` : ' | Perk already active';
    }

    await member.save();
    rewardLines.push(`<@${member.userId}> +${auraResult.reward} Aura | +${xpResult.reward} XP${perkText}`);
  }

  const clanXpResult = await addClanXp(raid.clanName, raid.boss.clanXp);
  return { rewardLines, clanXpResult, topDamagerId };
}

// ================= COMMAND HANDLER =================
client.on('messageCreate', async (message) => {
  if (message.author.bot) return;

  const user = await getUser(message.author.id);
  removeExpiredBoosts(user);
  const vaultInterest = applyVaultInterest(user);
  if (vaultInterest.applied > 0) {
    await user.save();
  }

  if (message.content === '!help') {
    const categories = [
      {
        name: 'Core',
        commands: COMMAND_INFO.filter(command => ['!help', '!spin', '!coinflip <heads/tails> <amount>', '!bal', '!rank', '!level', '!leaderboard', '!inv', '!stats'].includes(command.name))
      },
      {
        name: 'Skills & Shop',
        commands: COMMAND_INFO.filter(command => command.name.startsWith('!skill') || ['!shop', '!buy <item>', '!use <item>', '!open'].includes(command.name))
      },
      {
        name: 'Clans',
        commands: COMMAND_INFO.filter(command => command.name.startsWith('!clan'))
      },
      {
        name: 'Bosses',
        commands: COMMAND_INFO.filter(command => command.name.startsWith('!boss'))
      }
    ];

    const embed = createEmbed(message, 'Command Guide', EMBED_COLORS.info)
      .setDescription('Everything available in your bot right now.')
      .addFields(
        categories.map(category => field(
          category.name,
          category.commands.map(command => `\`${command.name}\`\n${command.description}`).join('\n\n'),
          false
        ))
      );

    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!spin') {
    const cd = cooldown(user, 'spin', 300000);
    if (cd) return message.reply({ embeds: [warningEmbed(message, 'Cooldown Active', `Wait ${cd}s before using \`!spin\` again.`)] });

    const s = ['Cherry', 'Gem', 'Fire', 'Lemon'];
    const r = [s[Math.floor(Math.random() * 4)], s[Math.floor(Math.random() * 4)], s[Math.floor(Math.random() * 4)]];

    const auraResult = addAura(user, 1000 + Math.floor(Math.random() * 4000), 'aura');
    const xpResult = addXp(user, 40 + Math.floor(Math.random() * 30));
    const clanXpResult = await addClanXp(user.clan, 20);
    xpResult.level = user.level;

    await user.save();
    const embed = createEmbed(message, 'Slot Spin', EMBED_COLORS.success)
      .setDescription(`Roll: ${r.join(' | ')}`)
      .addFields(
        field('Aura Won', `+${auraResult.reward}${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}`),
        field('XP', `+${xpResult.reward}`),
        field('Clan XP', `+${clanXpResult.gained || 0}`),
        field('Progress', `${levelProgress(user)}${user.clan ? `\n${clanLevelProgress(user)}` : ''}`, false)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content.startsWith('!coinflip')) {
    const cd = cooldown(user, 'cf', 120000);
    if (cd) return message.reply({ embeds: [warningEmbed(message, 'Cooldown Active', `Wait ${cd}s before using \`!coinflip\` again.`)] });

    const args = message.content.split(' ');
    if (!args[1] || !['heads', 'tails'].includes(args[1].toLowerCase())) {
        return message.reply({ embeds: [infoEmbed(message, 'Coinflip Usage', "Choose `heads` or `tails`.\nExample: `!coinflip heads 2000`")] });
    }

    const choice = args[1].toLowerCase();
    let bet = parseInt(args[2], 10);
    if (!bet || bet <= 0) return message.reply({ embeds: [warningEmbed(message, 'Invalid Bet', 'Enter a valid amount to bet.')] });
    if (bet > user.aura) bet = user.aura;

    const outcome = Math.random() > 0.5 ? 'heads' : 'tails';
    const won = choice === outcome;

    let auraWon = 0;
    let xpWon = 0;
    let clanXpWon = 0;

    if (won) {
      const auraResult = addAura(user, bet, 'aura');
      const xpResult = addXp(user, 60);
      const clanXpResult = await addClanXp(user.clan, 30);
      xpResult.level = user.level;

      auraWon = auraResult.reward;
      xpWon = xpResult.reward;
      clanXpWon = clanXpResult.gained || 0;
    }

    await user.save();
    const embed = createEmbed(message, 'Coinflip', won ? EMBED_COLORS.success : EMBED_COLORS.danger)
      .setDescription(`You chose **${choice}**. The coin landed on **${outcome}**.`)
      .addFields(
        field('Result', won ? 'Win' : 'Loss'),
        field('Aura', won ? `+${auraWon}` : 'No reward'),
        field('XP / Clan XP', won ? `+${xpWon} XP • +${clanXpWon} Clan XP` : 'No reward', false)
      );
    return message.reply({ embeds: [embed] });
}

  if (message.content.startsWith('!pvp')) {
    const targetUser = message.mentions.users.first();
    if (!targetUser) return message.reply({ embeds: [infoEmbed(message, 'PvP Usage', 'Mention a user to challenge.\nExample: `!pvp @user`')] });
    if (targetUser.bot) return message.reply({ embeds: [warningEmbed(message, 'Invalid Target', 'You cannot challenge bots.')] });
    if (targetUser.id === message.author.id) return message.reply({ embeds: [warningEmbed(message, 'Invalid Target', 'You cannot challenge yourself.')] });
    if (battles.has(message.author.id) || battles.has(targetUser.id)) return message.reply({ embeds: [warningEmbed(message, 'Battle Busy', 'One of those players is already in a battle.')] });

    const challengeKey = `${message.author.id}:${targetUser.id}`;
    pendingChallenges.set(challengeKey, {
      challengerId: message.author.id,
      targetId: targetUser.id,
      createdAt: Date.now()
    });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`pvp_accept:${message.author.id}:${targetUser.id}`)
        .setLabel('Accept')
        .setStyle(ButtonStyle.Success),
      new ButtonBuilder()
        .setCustomId(`pvp_decline:${message.author.id}:${targetUser.id}`)
        .setLabel('Decline')
        .setStyle(ButtonStyle.Secondary)
    );

    const embed = createEmbed(message, 'PvP Challenge', EMBED_COLORS.danger)
      .setDescription(`<@${message.author.id}> challenged <@${targetUser.id}> to a duel.`);

    return message.reply({ embeds: [embed], components: [row] });
  }

  if (message.content === '!bal') {
    updateRank(user);
    const interestText = vaultInterest.applied > 0 ? ` | Vault interest applied: +${vaultInterest.applied}` : '';
    const embed = createEmbed(message, 'Balance Overview', EMBED_COLORS.success)
      .addFields(
        field('Wallet', user.aura),
        field('Vault', user.vault),
        field('Rank', user.rank),
        field('Clan', user.clan ? `${user.clan} (Lv ${user.clanLevel})` : 'None')
      );

    if (interestText) {
      embed.setDescription(`Vault interest applied: +${vaultInterest.applied} Aura`);
    }

    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!rank') {
    updateRank(user);
    const { totalAura, currentRank, nextRank } = getRankData(user);
    const embed = createEmbed(message, 'Rank Progress', EMBED_COLORS.royal)
      .setDescription(getRankRewardText(user))
      .addFields(
        field('Current Rank', currentRank.name),
        field('Total Aura', totalAura),
        field('Next Rank', nextRank ? nextRank.name : 'Maxed'),
        field('Progress', rankProgress(user), false)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!level') {
    const embed = createEmbed(message, 'Level Progress', EMBED_COLORS.info)
      .addFields(
        field('Level', user.level),
        field('XP', `${user.xp}/${xpNeededForLevel(user.level)}`),
        field('Skill Points', availableSkillPoints(user)),
        field('Progress', levelProgress(user), false)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!skills') {
    const embed = createEmbed(message, 'Skill Tree', EMBED_COLORS.royal)
      .setDescription('Use `!skill upgrade <branch>` to spend points and `!skill paths` to view specializations.')
      .addFields(
        field('Available Points', availableSkillPoints(user)),
        field('Damage', `${user.skills.dmg} (${getSpecialization(user, 'dmg') || 'no path'})`),
        field('Defense', `${user.skills.defense} (${getSpecialization(user, 'defense') || 'no path'})`),
        field('Luck', `${user.skills.luck} (${getSpecialization(user, 'luck') || 'no path'})`),
        field('Upgrade Costs', `dmg ${nextSkillCost(user, 'dmg')} • defense ${nextSkillCost(user, 'defense')} • luck ${nextSkillCost(user, 'luck')}`, false)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content.startsWith('!skill')) {
    const args = message.content.split(' ');
    const action = (args[1] || '').toLowerCase();
    const skillName = (args[2] || '').toLowerCase();

    if (action === 'paths') {
      return message.reply({ embeds: [infoEmbed(message, 'Skill Paths', `Skill specializations unlock at branch level 5.\n${specializationSummary()}`)] });
    }

    if (action === 'specialize') {
      const pathName = (args[3] || '').toLowerCase();
      if (!['dmg', 'defense', 'luck'].includes(skillName) || !pathName) {
        return message.reply({ embeds: [infoEmbed(message, 'Skill Specialize Usage', 'Use `!skill specialize <dmg|defense|luck> <path>`.\nTry `!skill paths` first.')] });
      }
      if (!SKILL_SPECIALIZATIONS[skillName][pathName]) {
        return message.reply({ embeds: [warningEmbed(message, 'Invalid Specialization', 'That specialization does not exist for the chosen branch. Use `!skill paths`.')] });
      }
      if (user.skills[skillName] < 5) {
        return message.reply({ embeds: [warningEmbed(message, 'Branch Too Low', `You need ${skillName} level 5 to unlock a specialization.`)] });
      }
      if (getSpecialization(user, skillName)) {
        return message.reply({ embeds: [warningEmbed(message, 'Already Specialized', `You already chose ${getSpecialization(user, skillName)} for ${skillName}.`)] });
      }

      user.specializations[skillName] = pathName;
      user.markModified('specializations');
      await user.save();
      return message.reply({ embeds: [createEmbed(message, 'Specialization Chosen', EMBED_COLORS.success).setDescription(`Specialized **${skillName}** into **${pathName}**.\n${SKILL_SPECIALIZATIONS[skillName][pathName]}`)] });
    }

    if (action !== 'upgrade' || !['dmg', 'defense', 'luck'].includes(skillName)) {
      return message.reply({ embeds: [infoEmbed(message, 'Skill Usage', 'Use `!skill upgrade <dmg|defense|luck>`, `!skill specialize <branch> <path>`, or `!skill paths`.')] });
    }

    const points = availableSkillPoints(user);
    const cost = nextSkillCost(user, skillName);
    if (points < cost) {
      return message.reply({ embeds: [warningEmbed(message, 'Not Enough Skill Points', `You need ${cost} skill points to upgrade ${skillName}. Available: ${points}.`)] });
    }

    user.skills[skillName] += 1;
    user.markModified('skills');
    await user.save();
    return message.reply({ embeds: [createEmbed(message, 'Skill Upgraded', EMBED_COLORS.success).setDescription(`Upgraded **${skillName}** to **${user.skills[skillName]}**.`).addFields(field('Tree Summary', skillTreeSummary(user), false))] });
  }

  if (message.content.startsWith('!deposit')) {
    const amountArg = message.content.split(' ')[1];
    if (!amountArg) return message.reply({ embeds: [infoEmbed(message, 'Deposit Usage', 'Enter an amount to deposit.\nExample: `!deposit 5000` or `!deposit all`')] });

    const amount = amountArg.toLowerCase() === 'all' ? user.aura : parseInt(amountArg, 10);
    if (!Number.isInteger(amount) || amount <= 0) {
      return message.reply({ embeds: [warningEmbed(message, 'Invalid Amount', 'Enter a valid deposit amount.')] });
    }

    if (amount > user.aura) {
      return message.reply({ embeds: [warningEmbed(message, 'Not Enough Aura', "You don't have that much Aura in your wallet.")] });
    }

    user.aura -= amount;
    user.vault += amount;
    user.lastVaultInterest = Date.now();
    updateRank(user);
    await user.save();
    const embed = createEmbed(message, 'Vault Deposit', EMBED_COLORS.success)
      .setDescription(`Deposited ${amount} Aura into your vault.`)
      .addFields(
        field('Wallet', user.aura),
        field('Vault', user.vault),
        field('Base Interest', `${Math.round(VAULT_INTEREST_RATE * 100)}% every 24h`)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!daily') {
    const cd = cooldown(user, 'daily', 86400000);
    if (cd) return message.reply({ embeds: [warningEmbed(message, 'Cooldown Active', `Wait ${cd}s before claiming \`!daily\` again.`)] });

    user.streak++;
    const auraResult = addAura(user, 5000 * user.streak, 'daily');
    const xpResult = addXp(user, 120 + user.streak * 10);
    const clanXpResult = await addClanXp(user.clan, 50 + user.streak * 5);
    xpResult.level = user.level;

    await user.save();
    const embed = createEmbed(message, 'Daily Reward', EMBED_COLORS.success)
      .setDescription(`Your streak is now **${user.streak}**.`)
      .addFields(
        field('Aura', `+${auraResult.reward}${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}`),
        field('XP', `+${xpResult.reward}`),
        field('Clan XP', `+${clanXpResult.gained || 0}`),
        field('Progress', `${levelProgress(user)}${user.clan ? `\n${clanLevelProgress(user)}` : ''}`, false)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!vaultinterest') {
    const nextIn = Math.max(VAULT_INTEREST_INTERVAL - (Date.now() - user.lastVaultInterest), 0);
    const appliedText = vaultInterest.applied > 0 ? `Applied now: +${vaultInterest.applied} Aura | ` : '';
    const effectiveRate = ((vaultInterest.rate || (VAULT_INTEREST_RATE + getRankPerks(user).vaultBonus + getClanPerks(user).vaultBonus)) * 100).toFixed(2);
    const embed = createEmbed(message, 'Vault Interest', EMBED_COLORS.success)
      .setDescription(appliedText ? `Interest applied: +${vaultInterest.applied} Aura` : 'Your vault grows passively over time.')
      .addFields(
        field('Vault', user.vault),
        field('Rate', `${effectiveRate}% every 24h`),
        field('Next Payout', formatDuration(nextIn))
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!shop') {
    const embed = createEmbed(message, 'Aura Shop', EMBED_COLORS.primary)
      .setDescription('Boost items are capped and cannot stack while active.')
      .addFields(
        Object.entries(SHOP_ITEMS).map(([key, item]) =>
          field(`${item.name} • ${item.price} Aura`, `Key: \`${key}\`\nLimit: ${item.maxOwned}\n${item.description}`, false)
        )
      );

    return message.reply({ embeds: [embed] });
  }

  if (message.content.startsWith('!buy')) {
    const itemKey = (message.content.split(' ')[1] || '').toLowerCase();
    const shopItem = SHOP_ITEMS[itemKey];

    if (!shopItem) return message.reply({ embeds: [warningEmbed(message, 'Unknown Item', 'That item is not in the shop.')] });
    if (user.aura < shopItem.price) return message.reply({ embeds: [warningEmbed(message, 'Not Enough Aura', 'You do not have enough Aura for that purchase.')] });

    const ownedCount = countItem(user, shopItem.name);
    if (ownedCount >= shopItem.maxOwned) {
      return message.reply({ embeds: [warningEmbed(message, 'Item Cap Reached', `You already have the max allowed ${shopItem.name} (${shopItem.maxOwned}).`)] });
    }

    user.aura -= shopItem.price;
    user.inventory.push(shopItem.name);
    updateRank(user);
    await user.save();
    const embed = createEmbed(message, 'Purchase Complete', EMBED_COLORS.success)
      .setDescription(`Bought **${shopItem.name}** for ${shopItem.price} Aura.`)
      .addFields(
        field('Wallet Left', user.aura),
        field('Owned', `${countItem(user, shopItem.name)}`),
        field('Limit', shopItem.maxOwned)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content.startsWith('!use')) {
    const itemKey = (message.content.split(' ')[1] || '').toLowerCase();
    const shopItem = SHOP_ITEMS[itemKey];

    if (!shopItem || !shopItem.boostKey) {
      return message.reply({ embeds: [warningEmbed(message, 'Item Not Usable', 'That item cannot be used.')] });
    }

    if (!hasItem(user, shopItem.name)) {
      return message.reply({ embeds: [warningEmbed(message, 'Missing Item', `You do not have ${shopItem.name}.`)] });
    }

    if (isBoostActive(user, shopItem.boostKey)) {
      return message.reply({ embeds: [warningEmbed(message, 'Boost Already Active', `${BOOST_LABELS[shopItem.boostKey]} is already active. It cannot stack.`)] });
    }

    removeItem(user, shopItem.name);
    activateBoost(user, shopItem.boostKey, shopItem.multiplier, shopItem.durationMs);
    await user.save();
    const embed = createEmbed(message, 'Boost Activated', EMBED_COLORS.success)
      .setDescription(`${shopItem.name} is now active.`)
      .addFields(
        field('Effect', `${shopItem.multiplier}x ${BOOST_LABELS[shopItem.boostKey]}`),
        field('Duration', formatDuration(shopItem.durationMs))
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!open') {
    if (!hasItem(user, 'Crate')) return message.reply({ embeds: [warningEmbed(message, 'No Crate', 'You do not have a crate to open.')] });

    removeItem(user, 'Crate');
    const auraResult = addAura(user, Math.floor(Math.random() * 8000), 'crate');
    const xpResult = addXp(user, 50 + Math.floor(Math.random() * 30));
    const clanXpResult = await addClanXp(user.clan, 25);
    xpResult.level = user.level;

    await user.save();
    const embed = createEmbed(message, 'Crate Opened', EMBED_COLORS.primary)
      .addFields(
        field('Aura', `+${auraResult.reward}${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}`),
        field('XP', `+${xpResult.reward}`),
        field('Clan XP', `+${clanXpResult.gained || 0}`)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content.startsWith('!boss')) {
    const args = message.content.split(' ').slice(1);
    const subcommand = (args[0] || '').toLowerCase();

    if (!subcommand) {
      return message.reply({ embeds: [infoEmbed(message, 'Boss Commands', 'Use `!boss list`, `!boss start <boss>`, `!boss attack`, `!boss heal`, `!boss status`, or `!boss clan ...`.')] });
    }

    if (subcommand === 'list') {
      const embed = createEmbed(message, 'Boss Codex', EMBED_COLORS.danger)
        .setDescription('Solo and clan raids both use these bosses.')
        .addFields(
          BOSS_TEMPLATES.map(boss =>
            field(
              `${boss.name} • Lv ${boss.minLevel}`,
              `Key: \`${boss.key}\`\n${boss.description}\nSolo HP: ${boss.hp} • Clan HP: ${boss.clanHp}\nRewards: ${boss.aura} Aura, ${boss.xp} XP, ${boss.clanXp} Clan XP\nPerk: ${boss.perk.label}`,
              false
            )
          )
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'start') {
      const boss = findBoss(args.slice(1).join(' '));
      if (!boss) return message.reply({ embeds: [warningEmbed(message, 'Unknown Boss', 'Choose a valid boss. Use `!boss list`.')] });
      if (activeBossBattles.has(user.userId)) return message.reply({ embeds: [warningEmbed(message, 'Boss Fight Active', 'You already have an active solo boss fight.')] });
      if (user.level < boss.minLevel) return message.reply({ embeds: [warningEmbed(message, 'Level Too Low', `You need to be level ${boss.minLevel} to fight ${boss.name}.`)] });

      const maxPlayerHp = battleMaxHp(user);
      activeBossBattles.set(user.userId, {
        playerId: user.userId,
        boss,
        bossHp: boss.hp,
        playerHp: maxPlayerHp,
        maxPlayerHp,
        healsLeft: 3
      });

      const state = activeBossBattles.get(user.userId);
      const embed = createEmbed(message, `Solo Boss: ${boss.name}`, EMBED_COLORS.danger)
        .setDescription(boss.description)
        .addFields(
          field('Boss HP', `${state.bossHp}/${boss.hp}`),
          field('Your HP', `${state.playerHp}/${state.maxPlayerHp}`),
          field('Heals Left', state.healsLeft),
          field('Rewards', `${boss.aura} Aura • ${boss.xp} XP • ${boss.clanXp} Clan XP`, false),
          field('Perk', boss.perk.label, false)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'attack') {
      const state = activeBossBattles.get(user.userId);
      if (!state) return message.reply({ embeds: [warningEmbed(message, 'No Boss Fight', 'You do not have an active solo boss fight. Use `!boss start <boss>`.')] });

      const hit = bossPlayerAttack(user, state.boss);
      state.bossHp -= hit.damage;

      if (state.bossHp <= 0) {
        const auraResult = addAura(user, state.boss.aura, 'aura');
        const xpResult = addXp(user, state.boss.xp);
        const clanXpResult = await addClanXp(user.clan, state.boss.clanXp);
        const perkGranted = activateBoost(user, state.boss.perk.key, state.boss.perk.multiplier, state.boss.perk.durationMs);
        xpResult.level = user.level;
        await user.save();
        activeBossBattles.delete(user.userId);

        const embed = createEmbed(message, `Boss Defeated: ${state.boss.name}`, EMBED_COLORS.success)
          .setDescription(`Final hit: ${hit.damage}${hit.crit ? ' (CRIT)' : ''}`)
          .addFields(
            field('Aura', `+${auraResult.reward}`),
            field('XP', `+${xpResult.reward}`),
            field('Clan XP', `+${clanXpResult.gained || 0}`),
            field('Perk', perkGranted ? state.boss.perk.label : `${state.boss.perk.label} already active`, false)
          );
        return message.reply({ embeds: [embed] });
      }

      const counter = bossCounterDamage(user, state.boss);
      state.playerHp -= counter.damage;

      if (state.playerHp <= 0) {
        activeBossBattles.delete(user.userId);
        const embed = createEmbed(message, `Defeat: ${state.boss.name}`, EMBED_COLORS.danger)
          .setDescription(`The boss finished you with ${counter.dodged ? 'a missed counter' : `${counter.damage} damage`}.`)
          .addFields(field('Try Again', `Use \`!boss start ${state.boss.key}\``));
        return message.reply({ embeds: [embed] });
      }

      const embed = createEmbed(message, `Boss Turn: ${state.boss.name}`, EMBED_COLORS.danger)
        .setDescription(`You dealt ${hit.damage}${hit.crit ? ' (CRIT)' : ''}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
        .addFields(
          field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
          field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
          field('Heals Left', state.healsLeft)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'heal') {
      const state = activeBossBattles.get(user.userId);
      if (!state) return message.reply({ embeds: [warningEmbed(message, 'No Boss Fight', 'You do not have an active solo boss fight.')] });
      if (state.healsLeft <= 0) return message.reply({ embeds: [warningEmbed(message, 'No Heals Left', 'You have no heals left in this boss fight.')] });
      if (state.playerHp >= state.maxPlayerHp) return message.reply({ embeds: [warningEmbed(message, 'HP Full', 'Your HP is already full.')] });

      const healed = bossHealAmount(user, state.maxPlayerHp - state.playerHp);
      state.playerHp += healed;
      state.healsLeft -= 1;

      const counter = bossCounterDamage(user, state.boss);
      state.playerHp -= counter.damage;

      if (state.playerHp <= 0) {
        activeBossBattles.delete(user.userId);
        const embed = createEmbed(message, `Defeat: ${state.boss.name}`, EMBED_COLORS.danger)
          .setDescription(`You healed for ${healed}, but the boss ended the fight ${counter.dodged ? 'without landing the counter cleanly' : `with ${counter.damage} damage`}.`);
        return message.reply({ embeds: [embed] });
      }

      const embed = createEmbed(message, `Boss Heal: ${state.boss.name}`, EMBED_COLORS.success)
        .setDescription(`You healed for ${healed}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
        .addFields(
          field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
          field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
          field('Heals Left', state.healsLeft)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'status') {
      const state = activeBossBattles.get(user.userId);
      if (!state) return message.reply({ embeds: [warningEmbed(message, 'No Boss Fight', 'You do not have an active solo boss fight.')] });
      const embed = createEmbed(message, `Solo Boss: ${state.boss.name}`, EMBED_COLORS.danger)
        .addFields(
          field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
          field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
          field('Heals Left', state.healsLeft),
          field('Reward Perk', state.boss.perk.label)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'clan') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You need to be in a clan to use clan boss raids.')] });
      const action = (args[1] || '').toLowerCase();

      if (action === 'start') {
        if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can start a clan boss raid.')] });
        if (getActiveClanBossRaid(user.clan)) return message.reply({ embeds: [warningEmbed(message, 'Raid Already Active', 'Your clan already has an active boss raid.')] });

        const boss = findBoss(args.slice(2).join(' '));
        if (!boss) return message.reply({ embeds: [warningEmbed(message, 'Unknown Boss', 'Choose a valid boss. Use `!boss list`.')] });

        const requiredClanLevel = Math.max(1, Math.ceil(boss.minLevel / 2));
        if ((user.clanLevel || 1) < requiredClanLevel) {
          return message.reply({ embeds: [warningEmbed(message, 'Clan Level Too Low', `Your clan needs to be level ${requiredClanLevel} to challenge ${boss.name}.`)] });
        }

        activeClanBossRaids.set(user.clan, {
          clanName: user.clan,
          ownerId: user.userId,
          clanLevel: user.clanLevel || 1,
          boss,
          bossHp: boss.clanHp,
          participants: [],
          playerHp: {},
          playerMaxHp: {},
          healsLeft: {},
          damageByUser: {},
          logs: [`Raid started against ${boss.name}. Members can join with !boss clan join.`]
        });

        const embed = createEmbed(message, `Clan Raid Started: ${boss.name}`, EMBED_COLORS.danger)
          .setDescription(`**${user.clan}** has opened a raid against **${boss.name}**.`)
          .addFields(
            field('Participant Cap', clanBossParticipantCap(user.clanLevel || 1)),
            field('Boss HP', boss.clanHp),
            field('Join', 'Use `!boss clan join`', false)
          );
        return message.reply({ embeds: [embed] });
      }

      if (action === 'join') {
        const raid = getActiveClanBossRaid(user.clan);
        if (!raid) return message.reply({ embeds: [warningEmbed(message, 'No Active Raid', 'Your clan does not have an active boss raid.')] });

        const cap = clanBossParticipantCap(raid.clanLevel || 1);
        if (raid.participants.includes(user.userId)) return message.reply({ embeds: [warningEmbed(message, 'Already Joined', 'You are already in the clan boss raid.')] });
        if (raid.participants.length >= cap) return message.reply({ embeds: [warningEmbed(message, 'Raid Full', `The clan boss raid is full. Cap: ${cap}.`)] });

        raid.participants.push(user.userId);
        raid.playerMaxHp[user.userId] = battleMaxHp(user);
        raid.playerHp[user.userId] = raid.playerMaxHp[user.userId];
        raid.healsLeft[user.userId] = 3;
        raid.damageByUser[user.userId] = raid.damageByUser[user.userId] || 0;
        raid.logs.push(`${message.author.username} joined the raid.`);
        const embed = createEmbed(message, 'Raid Joined', EMBED_COLORS.success)
          .setDescription(`You joined the clan boss raid for **${user.clan}**.`)
          .addFields(
            field('Your HP', `${raid.playerHp[user.userId]}/${raid.playerMaxHp[user.userId]}`),
            field('Heals Left', raid.healsLeft[user.userId]),
            field('Roster Size', raid.participants.length)
          );
        return message.reply({ embeds: [embed] });
      }

      if (action === 'status') {
        const raid = getActiveClanBossRaid(user.clan);
        if (!raid) return message.reply({ embeds: [warningEmbed(message, 'No Active Raid', 'Your clan does not have an active boss raid.')] });
        const roster = raid.participants.length > 0
          ? raid.participants.map(id => `<@${id}> • ${Math.max(raid.playerHp[id] || 0, 0)}/${raid.playerMaxHp[id] || 1}`).join('\n')
          : 'No participants yet.';
        const embed = createEmbed(message, `Clan Raid: ${raid.boss.name}`, EMBED_COLORS.danger)
          .setDescription(`Clan: **${raid.clanName}**`)
          .addFields(
            field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
            field('Participants', raid.participants.length),
            field('Top Logs', raid.logs.slice(-4).join('\n') || 'No logs yet.', false),
            field('Roster', roster, false)
          );
        return message.reply({ embeds: [embed] });
      }

      if (action === 'attack') {
        const raid = getActiveClanBossRaid(user.clan);
        if (!raid) return message.reply({ embeds: [warningEmbed(message, 'No Active Raid', 'Your clan does not have an active boss raid.')] });
        if (!raid.participants.includes(user.userId)) return message.reply({ embeds: [infoEmbed(message, 'Join The Raid', 'Join the raid first with `!boss clan join`.')] });
        if ((raid.playerHp[user.userId] || 0) <= 0) return message.reply({ embeds: [warningEmbed(message, 'Raid KO', 'You are down in this raid and cannot attack.')] });

        const hit = bossPlayerAttack(user, raid.boss);
        raid.bossHp -= hit.damage;
        raid.damageByUser[user.userId] = (raid.damageByUser[user.userId] || 0) + hit.damage;
        raid.logs.push(`${message.author.username} dealt ${hit.damage}${hit.crit ? ' crit' : ''} damage.`);

        if (raid.bossHp <= 0) {
          const rewards = await rewardClanBossRaid(raid);
          activeClanBossRaids.delete(raid.clanName);

          const embed = createEmbed(message, `Clan Boss Defeated: ${raid.boss.name}`, EMBED_COLORS.success)
            .setDescription(`**${raid.clanName}** cleared the raid.`)
            .addFields(
              field('Top Damage', rewards.topDamagerId ? `<@${rewards.topDamagerId}>` : 'None'),
              field('Perk Awarded', raid.boss.perk.label),
              field('Clan XP', `+${rewards.clanXpResult.gained || 0}`),
              field('Rewards', rewards.rewardLines.join('\n'), false)
            );
          return message.reply({ embeds: [embed] });
        }

        const counter = bossCounterDamage(user, raid.boss);
        raid.playerHp[user.userId] -= counter.damage;
        raid.logs.push(counter.dodged ? `${message.author.username} dodged the counter.` : `${message.author.username} took ${counter.damage} counter damage.`);

        const livingMembers = raid.participants.filter(id => (raid.playerHp[id] || 0) > 0);
        if (livingMembers.length === 0) {
          activeClanBossRaids.delete(raid.clanName);
          const embed = createEmbed(message, `Raid Failed: ${raid.boss.name}`, EMBED_COLORS.danger)
            .setDescription(`The full **${raid.clanName}** roster was defeated.`);
          return message.reply({ embeds: [embed] });
        }

        const embed = createEmbed(message, `Raid Turn: ${raid.boss.name}`, EMBED_COLORS.danger)
          .setDescription(`You dealt ${hit.damage}${hit.crit ? ' (CRIT)' : ''}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
          .addFields(
            field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
            field('Your HP', `${Math.max(raid.playerHp[user.userId], 0)}/${raid.playerMaxHp[user.userId]}`),
            field('Roster Alive', raid.participants.filter(id => (raid.playerHp[id] || 0) > 0).length)
          );
        return message.reply({ embeds: [embed] });
      }

      if (action === 'heal') {
        const raid = getActiveClanBossRaid(user.clan);
        if (!raid) return message.reply({ embeds: [warningEmbed(message, 'No Active Raid', 'Your clan does not have an active boss raid.')] });
        if (!raid.participants.includes(user.userId)) return message.reply({ embeds: [infoEmbed(message, 'Join The Raid', 'Join the raid first with `!boss clan join`.')] });
        if ((raid.playerHp[user.userId] || 0) <= 0) return message.reply({ embeds: [warningEmbed(message, 'Raid KO', 'You are down in this raid and cannot heal.')] });
        if ((raid.healsLeft[user.userId] || 0) <= 0) return message.reply({ embeds: [warningEmbed(message, 'No Heals Left', 'You have no heals left in this raid.')] });

        const maxHp = raid.playerMaxHp[user.userId];
        if (raid.playerHp[user.userId] >= maxHp) return message.reply({ embeds: [warningEmbed(message, 'HP Full', 'Your HP is already full.')] });

        const healed = bossHealAmount(user, maxHp - raid.playerHp[user.userId]);
        raid.playerHp[user.userId] += healed;
        raid.healsLeft[user.userId] -= 1;

        const counter = bossCounterDamage(user, raid.boss);
        raid.playerHp[user.userId] -= counter.damage;
        raid.logs.push(`${message.author.username} healed for ${healed}.`);

        const livingMembers = raid.participants.filter(id => (raid.playerHp[id] || 0) > 0);
        if (livingMembers.length === 0) {
          activeClanBossRaids.delete(raid.clanName);
          const embed = createEmbed(message, `Raid Failed: ${raid.boss.name}`, EMBED_COLORS.danger)
            .setDescription(`You healed for ${healed}, but the raid still collapsed.`);
          return message.reply({ embeds: [embed] });
        }

        const embed = createEmbed(message, `Raid Heal: ${raid.boss.name}`, EMBED_COLORS.success)
          .setDescription(`You healed for ${healed}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
          .addFields(
            field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
            field('Your HP', `${Math.max(raid.playerHp[user.userId], 0)}/${raid.playerMaxHp[user.userId]}`),
            field('Heals Left', raid.healsLeft[user.userId])
          );
        return message.reply({ embeds: [embed] });
      }

      return message.reply({ embeds: [infoEmbed(message, 'Clan Boss Commands', 'Use `!boss clan start <boss>`, `!boss clan join`, `!boss clan attack`, `!boss clan heal`, or `!boss clan status`.')] });
    }

    return message.reply({ embeds: [warningEmbed(message, 'Unknown Boss Command', 'Use `!boss list`, `!boss start <boss>`, `!boss attack`, `!boss heal`, `!boss status`, or `!boss clan ...`.')] });
  }

  if (message.content.startsWith('!clan')) {
    const args = message.content.split(' ').slice(1);
    const subcommand = (args[0] || '').toLowerCase();

    if (!subcommand) {
      return message.reply({ embeds: [infoEmbed(message, 'Clan Commands', 'Use `!clan create <name>`, `!clan invite @user`, `!clan join <name>`, `!clan accept <name>`, `!clan decline <name>`, `!clan leave`, `!clan transfer @user`, `!clan kick @user`, `!clan war <name>`, `!clan info [name]`, or `!clan top`.')] });
    }

    if (subcommand === 'create') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply({ embeds: [infoEmbed(message, 'Clan Create Usage', 'Enter a clan name.\nExample: `!clan create Shadow Guild`')] });
      if (!isValidClanName(clanName)) return message.reply({ embeds: [warningEmbed(message, 'Invalid Clan Name', 'Clan names must be 3-20 characters and use only letters, numbers, and spaces.')] });
      if (user.clan) return message.reply({ embeds: [warningEmbed(message, 'Already In Clan', `You are already in **${user.clan}**. Leave it first to create a new clan.`)] });
      if (user.aura < CLAN_CREATE_COST) return message.reply({ embeds: [warningEmbed(message, 'Not Enough Aura', `You need ${CLAN_CREATE_COST} Aura to create a clan.`)] });

      const existingClan = await User.findOne({ clan: new RegExp(`^${clanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') });
      if (existingClan) return message.reply({ embeds: [warningEmbed(message, 'Clan Name Taken', 'That clan name is already taken.')] });

      user.aura -= CLAN_CREATE_COST;
      user.clan = clanName;
      user.clanRole = 'owner';
      user.clanInvites = [];
      user.clanLevel = 1;
      user.clanXp = 0;
      updateRank(user);
      await user.save();
      const embed = createEmbed(message, `Clan Created: ${clanName}`, EMBED_COLORS.success)
        .setDescription(`Your clan is live and ${CLAN_CREATE_COST} Aura has been spent.`)
        .addFields(
          field('Owner', `<@${user.userId}>`),
          field('Clan Level', user.clanLevel),
          field('Wallet Left', user.aura)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'invite') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You need to be in a clan to invite players.')] });
      if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can invite players.')] });

      const targetUser = message.mentions.users.first();
      if (!targetUser) return message.reply({ embeds: [infoEmbed(message, 'Clan Invite Usage', 'Mention a user to invite.\nExample: `!clan invite @user`')] });
      if (targetUser.bot) return message.reply({ embeds: [warningEmbed(message, 'Invalid Target', 'You cannot invite bots.')] });
      if (targetUser.id === message.author.id) return message.reply({ embeds: [warningEmbed(message, 'Invalid Target', 'You are already in the clan.')] });

      const target = await getUser(targetUser.id);
      if (target.clan) return message.reply({ embeds: [warningEmbed(message, 'Player Already In Clan', 'That player is already in a clan.')] });

      const clanMembers = await findClanMembers(user.clan);
      const clanPerks = getClanLevelData(user.clanLevel || 1).perks;
      if (clanMembers.length >= clanPerks.memberCap) {
        return message.reply({ embeds: [warningEmbed(message, 'Clan Full', `Your clan is full. Member cap: ${clanPerks.memberCap}.`)] });
      }

      if (!Array.isArray(target.clanInvites)) target.clanInvites = [];
      if (target.clanInvites.includes(user.clan)) {
        return message.reply({ embeds: [warningEmbed(message, 'Invite Already Sent', `${targetUser.username} already has an invite to **${user.clan}**.`)] });
      }

      target.clanInvites.push(user.clan);
      await target.save();
      const embed = createEmbed(message, 'Clan Invite Sent', EMBED_COLORS.info)
        .setDescription(`Invited **${targetUser.username}** to **${user.clan}**.`);
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'join') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply('Enter a clan name to join. Example: !clan join Shadow Guild');
      if (user.clan) return message.reply(`You are already in **${user.clan}**. Leave it first before joining another clan.`);

      const existingClan = await User.findOne({ clan: new RegExp(`^${clanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') });
      if (!existingClan) return message.reply('That clan does not exist.');
      if (!user.clanInvites.includes(existingClan.clan)) return message.reply(`You need an invite to join **${existingClan.clan}**. Use !clan accept ${existingClan.clan} after being invited.`);
      const clanPerks = getClanLevelData(existingClan.clanLevel || 1).perks;
      const memberCount = await User.countDocuments({ clan: existingClan.clan });
      if (memberCount >= clanPerks.memberCap) return message.reply(`**${existingClan.clan}** is full. Member cap: ${clanPerks.memberCap}.`);

      user.clan = existingClan.clan;
      user.clanRole = 'member';
      user.clanInvites = user.clanInvites.filter(invite => invite !== existingClan.clan);
      user.clanLevel = existingClan.clanLevel || 1;
      user.clanXp = existingClan.clanXp || 0;
      await user.save();
      const embed = createEmbed(message, 'Clan Joined', EMBED_COLORS.success)
        .setDescription(`You joined **${existingClan.clan}**.`)
        .addFields(
          field('Clan Level', user.clanLevel),
          field('Role', user.clanRole)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'accept') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply('Enter a clan name to accept. Example: !clan accept Shadow Guild');
      if (user.clan) return message.reply(`You are already in **${user.clan}**.`);

      const inviteName = user.clanInvites.find(invite => invite.toLowerCase() === clanName.toLowerCase());
      if (!inviteName) return message.reply('You do not have an invite to that clan.');

      const existingClan = await User.findOne({ clan: inviteName });
      if (!existingClan) {
        user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
        await user.save();
        return message.reply('That clan no longer exists.');
      }

      const clanPerks = getClanLevelData(existingClan.clanLevel || 1).perks;
      const memberCount = await User.countDocuments({ clan: existingClan.clan });
      if (memberCount >= clanPerks.memberCap) return message.reply(`**${existingClan.clan}** is full. Member cap: ${clanPerks.memberCap}.`);

      user.clan = existingClan.clan;
      user.clanRole = 'member';
      user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
      user.clanLevel = existingClan.clanLevel || 1;
      user.clanXp = existingClan.clanXp || 0;
      await user.save();
      const embed = createEmbed(message, 'Clan Invite Accepted', EMBED_COLORS.success)
        .setDescription(`You joined **${existingClan.clan}**.`)
        .addFields(
          field('Clan Level', user.clanLevel),
          field('Role', user.clanRole)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'decline') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply('Enter a clan name to decline. Example: !clan decline Shadow Guild');

      const inviteName = user.clanInvites.find(invite => invite.toLowerCase() === clanName.toLowerCase());
      if (!inviteName) return message.reply('You do not have an invite to that clan.');

      user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
      await user.save();
      const embed = createEmbed(message, 'Clan Invite Declined', EMBED_COLORS.danger)
        .setDescription(`Declined invite to **${inviteName}**.`);
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'leave') {
      if (!user.clan) return message.reply('You are not in a clan.');
      if (user.clanRole === 'owner') {
        const otherMembers = await User.countDocuments({ clan: user.clan, userId: { $ne: user.userId } });
        if (otherMembers > 0) {
          return message.reply('Transfer ownership before leaving your clan.');
        }
      }

      const oldClan = user.clan;
      const oldMembers = await User.countDocuments({ clan: oldClan });
      user.clan = null;
      user.clanRole = null;
      user.clanLevel = 1;
      user.clanXp = 0;
      await user.save();
      if (oldMembers === 1) {
        const embed = createEmbed(message, 'Clan Left', EMBED_COLORS.danger)
          .setDescription(`You left **${oldClan}**. The clan now has no members.`);
        return message.reply({ embeds: [embed] });
      }
      const embed = createEmbed(message, 'Clan Left', EMBED_COLORS.danger)
        .setDescription(`You left **${oldClan}**.`);
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'transfer') {
      if (!user.clan) return message.reply('You are not in a clan.');
      if (user.clanRole !== 'owner') return message.reply('Only the clan owner can transfer ownership.');

      const targetUser = message.mentions.users.first();
      if (!targetUser) return message.reply('Mention a clan member to transfer ownership to.');

      const target = await User.findOne({ userId: targetUser.id, clan: user.clan });
      if (!target) return message.reply('That user is not in your clan.');
      if (target.userId === user.userId) return message.reply('You already own the clan.');

      user.clanRole = 'member';
      target.clanRole = 'owner';
      await user.save();
      await target.save();
      const embed = createEmbed(message, 'Ownership Transferred', EMBED_COLORS.info)
        .setDescription(`Transferred **${user.clan}** ownership to **${targetUser.username}**.`);
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'kick') {
      if (!user.clan) return message.reply('You are not in a clan.');
      if (user.clanRole !== 'owner') return message.reply('Only the clan owner can kick members.');

      const targetUser = message.mentions.users.first();
      if (!targetUser) return message.reply('Mention a clan member to kick.');

      const target = await User.findOne({ userId: targetUser.id, clan: user.clan });
      if (!target) return message.reply('That user is not in your clan.');
      if (target.clanRole === 'owner') return message.reply('You cannot kick the clan owner.');

      target.clan = null;
      target.clanRole = null;
      target.clanLevel = 1;
      target.clanXp = 0;
      await target.save();
      const embed = createEmbed(message, 'Member Kicked', EMBED_COLORS.danger)
        .setDescription(`Removed **${targetUser.username}** from **${user.clan}**.`);
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'war') {
      if (!user.clan) return message.reply('You are not in a clan.');
      const warAction = (args[1] || '').toLowerCase();

      if (warAction === 'join') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply('Your clan does not have an active clan war.');
        if (war.started) return message.reply('This clan war has already started.');

        const rosterKey = war.attackerClan === user.clan ? 'attackerParticipants' : 'defenderParticipants';
        const clanLevel = war.attackerClan === user.clan ? war.attackerLevel : war.defenderLevel;
        const cap = clanWarParticipantCap(clanLevel);
        if (war[rosterKey].includes(user.userId)) return message.reply('You are already on the war roster.');
        if (war[rosterKey].length >= cap) return message.reply(`Your clan war roster is full. Cap: ${cap}.`);

        war[rosterKey].push(user.userId);
        war.logs.push(`${message.author.username} joined the ${user.clan} war roster.`);
        const embed = createEmbed(message, 'Clan War Roster', EMBED_COLORS.info)
          .setDescription(`You joined the war roster for **${user.clan}**.`)
          .addFields(field('Roster Size', war[rosterKey].length), field('Cap', cap));
        return message.reply({ embeds: [embed] });
      }

      if (warAction === 'leave') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply('Your clan does not have an active clan war.');
        if (war.started) return message.reply('This clan war has already started.');

        const rosterKey = war.attackerClan === user.clan ? 'attackerParticipants' : 'defenderParticipants';
        war[rosterKey] = war[rosterKey].filter(id => id !== user.userId);
        war.logs.push(`${message.author.username} left the ${user.clan} war roster.`);
        const embed = createEmbed(message, 'Clan War Roster', EMBED_COLORS.danger)
          .setDescription(`You left the war roster for **${user.clan}**.`);
        return message.reply({ embeds: [embed] });
      }

      if (warAction === 'status') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply('Your clan does not have an active clan war.');

        const attackerCap = clanWarParticipantCap(war.attackerLevel);
        const defenderCap = clanWarParticipantCap(war.defenderLevel);
        const recentLogs = war.logs.slice(-5).join('\n') || 'No logs yet.';

        const embed = createEmbed(message, 'Clan War Status', EMBED_COLORS.danger)
          .setDescription(`**${war.attackerClan}** vs **${war.defenderClan}**`)
          .addFields(
            field('Started', war.started ? 'Yes' : 'No'),
            field(`${war.attackerClan} Roster`, `${war.attackerParticipants.length}/${attackerCap}`),
            field(`${war.defenderClan} Roster`, `${war.defenderParticipants.length}/${defenderCap}`),
            field('Recent Logs', recentLogs, false)
          );
        return message.reply({ embeds: [embed] });
      }

      if (warAction === 'start') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply('Your clan does not have an active clan war.');
        if (user.clanRole !== 'owner') return message.reply('Only clan owners can start clan wars.');
        if (war.started) return message.reply('This clan war has already started.');
        if (user.userId !== war.attackerOwnerId && user.userId !== war.defenderOwnerId) {
          return message.reply('Only the two clan owners can start this war.');
        }

        if (war.attackerParticipants.length === 0 || war.defenderParticipants.length === 0) {
          return message.reply('Both clans need at least one participant before the war can start.');
        }

        war.started = true;
        const result = await resolveClanWar(war);
        activeClanWars.delete(war.key);

        const embed = createEmbed(message, 'Clan War Result', EMBED_COLORS.success)
          .setDescription(`Winner: **${result.winnerClan}**`)
          .addFields(
            field('Loser', result.loserClan),
            field('Score', `${war.attackerClan} ${result.attackerRounds} - ${war.defenderClan} ${result.defenderRounds}`),
            field('Rewards', `Winner members +6000 Aura • Loser members +2000 Aura • +${result.winnerClanXp.gained || 0} Clan XP`, false),
            field('War Log', war.logs.slice(-6).join('\n'), false)
          );
        return message.reply({ embeds: [embed] });
      }

      if (user.clanRole !== 'owner') return message.reply('Only the clan owner can start a clan war.');
      if (getActiveClanWar(user.clan)) return message.reply('Your clan already has an active clan war.');

      const targetClanName = normalizeClanName(args.slice(1).join(' '));
      if (!targetClanName) return message.reply('Enter a clan name to challenge. Example: !clan war Shadow Guild');
      if (targetClanName.toLowerCase() === user.clan.toLowerCase()) return message.reply('You cannot challenge your own clan.');

      const targetSummary = await getClanSummary(targetClanName);
      if (!targetSummary) return message.reply('That clan does not exist.');
      if (getActiveClanWar(targetSummary.name)) return message.reply('That clan already has an active clan war.');

      const targetOwner = targetSummary.members.find(member => member.clanRole === 'owner');
      if (!targetOwner) return message.reply('That clan does not currently have an owner.');

      const warKey = `${user.clan}:${targetSummary.name}`;
      pendingClanWars.set(warKey, {
        attackerClan: user.clan,
        defenderClan: targetSummary.name,
        attackerOwnerId: user.userId,
        defenderOwnerId: targetOwner.userId,
        createdAt: Date.now()
      });

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId(`clanwar_accept:${user.clan}:${targetSummary.name}`)
          .setLabel('Accept Clan War')
          .setStyle(ButtonStyle.Danger),
        new ButtonBuilder()
          .setCustomId(`clanwar_decline:${user.clan}:${targetSummary.name}`)
          .setLabel('Decline')
          .setStyle(ButtonStyle.Secondary)
      );

      const embed = createEmbed(message, 'Clan War Challenge', EMBED_COLORS.danger)
        .setDescription(`**${user.clan}** has challenged **${targetSummary.name}**.`)
        .addFields(field('Defending Owner', `<@${targetOwner.userId}>`));
      return message.reply({ embeds: [embed], components: [row] });
    }

    if (subcommand === 'info') {
      const requestedName = normalizeClanName(args.slice(1).join(' '));
      const clanName = requestedName || user.clan;
      if (!clanName) return message.reply('Enter a clan name or join a clan first. Example: !clan info Shadow Guild');

      const summary = await getClanSummary(clanName);
      if (!summary) return message.reply('That clan was not found.');
      const clanData = getClanLevelData(summary.members[0].clanLevel || 1);
      const clanOwner = summary.members.find(member => member.clanRole === 'owner');

      const memberText = summary.members
        .slice(0, 10)
        .map(member => `<@${member.userId}> - ${member.clanRole === 'owner' ? 'Owner' : 'Member'} | Aura: ${member.aura + member.vault} | Level: ${member.level}`)
        .join('\n');

      const embed = createEmbed(message, `Clan: ${summary.name}`, EMBED_COLORS.info)
        .setDescription(clanData.reward)
        .addFields(
          field('Owner', clanOwner ? `<@${clanOwner.userId}>` : 'None'),
          field('Members', `${summary.members.length}/${clanData.perks.memberCap}`),
          field('Total Aura', summary.totalAura),
          field('Total Wins', summary.totalWins),
          field('Power', Math.floor(getClanPower(summary))),
          field('Clan Progress', clanLevelProgress(summary.members[0]), false),
          field('Roster', memberText || 'No members', false)
        );
      return message.reply({ embeds: [embed] });
    }

    if (subcommand === 'top') {
      const clanUsers = await User.find({ clan: { $ne: null } }).sort({ clan: 1 });
      if (clanUsers.length === 0) return message.reply('No clans exist yet.');

      const clanMap = new Map();
      for (const member of clanUsers) {
        const current = clanMap.get(member.clan) || { aura: 0, members: 0, level: member.clanLevel || 1 };
        current.aura += member.aura + member.vault;
        current.members += 1;
        current.level = Math.max(current.level, member.clanLevel || 1);
        clanMap.set(member.clan, current);
      }

      const topClans = [...clanMap.entries()]
        .sort((a, b) => b[1].aura - a[1].aura)
        .slice(0, 5)
        .map(([name, data], index) => `${index + 1}. ${name} - Aura: ${data.aura} | Members: ${data.members} | Clan Level: ${data.level}`)
        .join('\n');

      const embed = createEmbed(message, 'Top Clans', EMBED_COLORS.info)
        .setDescription(topClans);
      return message.reply({ embeds: [embed] });
    }

    return message.reply({ embeds: [warningEmbed(message, 'Unknown Clan Command', 'Use `!clan create`, `!clan invite`, `!clan join`, `!clan accept`, `!clan decline`, `!clan leave`, `!clan transfer`, `!clan kick`, `!clan war <name>`, `!clan war join`, `!clan war leave`, `!clan war start`, `!clan war status`, `!clan info`, or `!clan top`.')] });
  }

  if (message.content === '!leaderboard') {
    const top = await User.find().sort({ aura: -1 }).limit(5);
    const text = top.map((u, i) => `${i + 1}. <@${u.userId}>: ${u.aura}`).join('\n');
    const embed = createEmbed(message, 'Top Players', EMBED_COLORS.primary)
      .setDescription(text || 'No players yet.');
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!inv') {
    const embed = createEmbed(message, 'Inventory', EMBED_COLORS.primary)
      .addFields(
        field('Items', inventorySummary(user), false),
        field('Active Boosts', activeBoostSummary(user), false)
      );
    return message.reply({ embeds: [embed] });
  }

  if (message.content === '!stats') {
    const embed = createEmbed(message, 'Player Stats', EMBED_COLORS.royal)
      .setDescription(getRankRewardText(user))
      .addFields(
        field('Rank', user.rank),
        field('Level', user.level),
        field('Clan', user.clan ? `${user.clan} (Lv ${user.clanLevel})` : 'None'),
        field('XP', `${user.xp}/${xpNeededForLevel(user.level)}`),
        field('Combat', `Wins ${user.wins} • Losses ${user.losses}`),
        field('Skills', `dmg ${user.skills.dmg} (${getSpecialization(user, 'dmg') || 'none'})\ndefense ${user.skills.defense} (${getSpecialization(user, 'defense') || 'none'})\nluck ${user.skills.luck} (${getSpecialization(user, 'luck') || 'none'})`, false),
        field('Boosts', activeBoostSummary(user), false)
      );
    return message.reply({ embeds: [embed] });
  }
});

// ================= BUTTON-BASED PVP =================
client.on('interactionCreate', async (interaction) => {
  if (!interaction.isButton()) return;

  const user = await getUser(interaction.user.id);
  removeExpiredBoosts(user);
  const vaultInterest = applyVaultInterest(user);
  if (vaultInterest.applied > 0) {
    await user.save();
  }

  if (interaction.customId.startsWith('clanwar_accept:')) {
    const [, attackerClanRaw, defenderClanRaw] = interaction.customId.split(':');
    const warKey = `${attackerClanRaw}:${defenderClanRaw}`;
    const war = pendingClanWars.get(warKey);
    if (!war) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Challenge Expired', 'This clan war challenge has expired or was already handled.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (interaction.user.id !== war.defenderOwnerId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Owner Only', 'Only the challenged clan owner can accept this clan war.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const attackerSummary = await getClanSummary(war.attackerClan);
    const defenderSummary = await getClanSummary(war.defenderClan);
    if (!attackerSummary || !defenderSummary) {
      pendingClanWars.delete(warKey);
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Missing', 'One of the clans no longer exists.', EMBED_COLORS.danger)], ephemeral: true });
    }

    pendingClanWars.delete(warKey);
    const activeWar = {
      key: warKey,
      attackerClan: war.attackerClan,
      defenderClan: war.defenderClan,
      attackerOwnerId: war.attackerOwnerId,
      defenderOwnerId: war.defenderOwnerId,
      attackerLevel: attackerSummary.members[0].clanLevel || 1,
      defenderLevel: defenderSummary.members[0].clanLevel || 1,
      attackerParticipants: [],
      defenderParticipants: [],
      started: false,
      logs: ['Clan war accepted.', `Owners can now build rosters with !clan war join and start with !clan war start.`]
    };
    activeClanWars.set(warKey, activeWar);

    return interaction.update({
      embeds: [
        combatEmbed(
          'Clan War Accepted',
          EMBED_COLORS.success,
          `**${war.attackerClan}** vs **${war.defenderClan}** is now active.`,
          [
            field(`${war.attackerClan} Cap`, clanWarParticipantCap(activeWar.attackerLevel)),
            field(`${war.defenderClan} Cap`, clanWarParticipantCap(activeWar.defenderLevel)),
            field('Next Step', 'Members use `!clan war join` and owners use `!clan war start`.', false)
          ]
        )
      ],
      components: []
    });
  }

  if (interaction.customId.startsWith('clanwar_decline:')) {
    const [, attackerClanRaw, defenderClanRaw] = interaction.customId.split(':');
    const warKey = `${attackerClanRaw}:${defenderClanRaw}`;
    const war = pendingClanWars.get(warKey);
    if (!war) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Challenge Closed', 'This clan war challenge has already been handled.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (interaction.user.id !== war.defenderOwnerId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Owner Only', 'Only the challenged clan owner can decline this clan war.', EMBED_COLORS.danger)], ephemeral: true });
    }

    pendingClanWars.delete(warKey);
    return interaction.update({
      embeds: [
        combatEmbed(
          'Clan War Declined',
          EMBED_COLORS.danger,
          `**${war.defenderClan}** refused the challenge from **${war.attackerClan}**.`
        )
      ],
      components: []
    });
  }

  if (interaction.customId.startsWith('pvp_accept:')) {
    const [, challengerId, targetId] = interaction.customId.split(':');
    if (interaction.user.id !== targetId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Challenge Locked', 'Only the challenged player can accept this fight.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const challengeKey = `${challengerId}:${targetId}`;
    const challenge = pendingChallenges.get(challengeKey);
    if (!challenge) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Challenge Expired', 'This challenge has expired or was already handled.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (battles.has(challengerId) || battles.has(targetId)) {
      pendingChallenges.delete(challengeKey);
      return interaction.reply({ embeds: [interactionNoticeEmbed('Battle Busy', 'One of the players is already in a battle.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const challenger = await getUser(challengerId);
    const challenged = await getUser(targetId);
    const firstTurn = Math.random() < 0.5 ? challengerId : targetId;

    const battle = {
      players: [challengerId, targetId],
      hp: {
        [challengerId]: battleMaxHp(challenger),
        [targetId]: battleMaxHp(challenged)
      },
      defending: {
        [challengerId]: false,
        [targetId]: false
      },
      healsLeft: {
        [challengerId]: 2,
        [targetId]: 2
      },
      critsLeft: {
        [challengerId]: 1,
        [targetId]: 1
      },
      turn: firstTurn
    };

    battles.set(challengerId, battle);
    battles.set(targetId, battle);
    pendingChallenges.delete(challengeKey);

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle('PvP Battle Started')
          .setDescription(`First turn: <@${firstTurn}>`)
          .addFields(
            field('Challenger HP', `${battle.hp[challengerId]}/${battleMaxHp(challenger)} ${bar(battle.hp[challengerId], battleMaxHp(challenger))}`, false),
            field('Opponent HP', `${battle.hp[targetId]}/${battleMaxHp(challenged)} ${bar(battle.hp[targetId], battleMaxHp(challenged))}`, false),
            field('Battle Rules', 'Each player has 2 heals and 1 critical strike.', false)
          )
          .setTimestamp()
      ],
      components: [buildBattleRow(false)]
    });
  }

  if (interaction.customId.startsWith('pvp_decline:')) {
    const [, challengerId, targetId] = interaction.customId.split(':');
    if (interaction.user.id !== targetId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Challenge Locked', 'Only the challenged player can decline this fight.', EMBED_COLORS.danger)], ephemeral: true });
    }

    pendingChallenges.delete(`${challengerId}:${targetId}`);
    return interaction.update({
      embeds: [
        combatEmbed(
          'PvP Challenge Declined',
          EMBED_COLORS.danger,
          `<@${targetId}> declined the PvP challenge from <@${challengerId}>.`
        )
      ],
      components: []
    });
  }

  if (interaction.customId === 'attack') {
    const battle = battles.get(interaction.user.id);
    if (!battle) return interaction.reply({ embeds: [interactionNoticeEmbed('No Battle', 'You are not currently in a battle.', EMBED_COLORS.danger)], ephemeral: true });
    if (battle.turn !== interaction.user.id) return interaction.reply({ embeds: [interactionNoticeEmbed('Wait Your Turn', 'It is not your turn yet.', EMBED_COLORS.info)], ephemeral: true });

    const enemyId = battle.players.find(id => id !== interaction.user.id);
    const attacker = await getUser(interaction.user.id);
    const defender = await getUser(enemyId);
    const result = resolveBattleDamage(attacker, defender, battle, 'attack');
    battle.turn = enemyId;

    if (battle.hp[enemyId] <= 0) {
      user.wins++;
      defender.losses++;
      const auraResult = addAura(user, 10000, 'aura');
      const xpResult = addXp(user, 150);
      const clanXpResult = await addClanXp(user.clan, 75);
      xpResult.level = user.level;

      await user.save();
      await defender.save();
      battles.delete(interaction.user.id);
      battles.delete(enemyId);

      return interaction.update({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.success)
            .setTitle('PvP Victory')
            .setDescription(`+${auraResult.reward} Aura${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}${formatProgressExtras(xpResult)}${formatClanProgressExtras(clanXpResult)}`)
            .setTimestamp()
        ],
        components: [buildBattleRow(true)]
      });
    }

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle('PvP Turn')
          .setDescription(result.dodged ? 'Enemy dodged your attack.' : `Damage dealt: ${result.damage}${result.crit ? ' (CRIT)' : ''}`)
          .addFields(
            field('Next Turn', `<@${enemyId}>`),
            field('Your HP', `${battle.hp[interaction.user.id]}/${battleMaxHp(attacker)} ${bar(battle.hp[interaction.user.id], battleMaxHp(attacker))}`, false),
            field('Enemy HP', `${Math.max(battle.hp[enemyId], 0)}/${battleMaxHp(defender)} ${bar(Math.max(battle.hp[enemyId], 0), battleMaxHp(defender))}`, false)
          )
          .setTimestamp()
      ],
      components: [buildBattleRow(false)]
    });
  }

  if (interaction.customId === 'heal') {
    const battle = battles.get(interaction.user.id);
    if (!battle) return interaction.reply({ embeds: [interactionNoticeEmbed('No Battle', 'You are not currently in a battle.', EMBED_COLORS.danger)], ephemeral: true });
    if (battle.turn !== interaction.user.id) return interaction.reply({ embeds: [interactionNoticeEmbed('Wait Your Turn', 'It is not your turn yet.', EMBED_COLORS.info)], ephemeral: true });

    const enemyId = battle.players.find(id => id !== interaction.user.id);
    const healer = await getUser(interaction.user.id);
    const maxHp = battleMaxHp(healer);

    if (battle.healsLeft[interaction.user.id] <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Heals Left', 'You have no heals left in this battle.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const missingHp = maxHp - battle.hp[interaction.user.id];
    if (missingHp <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('HP Full', 'Your HP is already full.', EMBED_COLORS.info)], ephemeral: true });
    }

    const healAmount = Math.min(missingHp, 1200 + healer.skills.defense * 200 + healer.skills.luck * 50);
    const medicBonus = getSpecialization(healer, 'defense') === 'medic' ? 1.25 : 1;
    const finalHeal = Math.min(missingHp, Math.floor(healAmount * medicBonus));
    battle.hp[interaction.user.id] += finalHeal;
    battle.healsLeft[interaction.user.id] -= 1;
    battle.defending[interaction.user.id] = false;
    battle.turn = enemyId;

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle('PvP Heal')
          .setDescription(`You restored ${finalHeal} HP.`)
          .addFields(
            field('Heals Left', battle.healsLeft[interaction.user.id]),
            field('Next Turn', `<@${enemyId}>`),
            field('Your HP', `${battle.hp[interaction.user.id]}/${maxHp} ${bar(battle.hp[interaction.user.id], maxHp)}`, false)
          )
          .setTimestamp()
      ],
      components: [buildBattleRow(false)]
    });
  }

  if (interaction.customId === 'defend') {
    const battle = battles.get(interaction.user.id);
    if (!battle) return interaction.reply({ embeds: [interactionNoticeEmbed('No Battle', 'You are not currently in a battle.', EMBED_COLORS.danger)], ephemeral: true });
    if (battle.turn !== interaction.user.id) return interaction.reply({ embeds: [interactionNoticeEmbed('Wait Your Turn', 'It is not your turn yet.', EMBED_COLORS.info)], ephemeral: true });

    const enemyId = battle.players.find(id => id !== interaction.user.id);
    battle.defending[interaction.user.id] = true;
    battle.turn = enemyId;

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.info)
          .setTitle('PvP Defend')
          .setDescription('Defend activated. Your next incoming hit will be reduced.')
          .addFields(field('Next Turn', `<@${enemyId}>`))
          .setTimestamp()
      ],
      components: [buildBattleRow(false)]
    });
  }

  if (interaction.customId === 'critical') {
    const battle = battles.get(interaction.user.id);
    if (!battle) return interaction.reply({ embeds: [interactionNoticeEmbed('No Battle', 'You are not currently in a battle.', EMBED_COLORS.danger)], ephemeral: true });
    if (battle.turn !== interaction.user.id) return interaction.reply({ embeds: [interactionNoticeEmbed('Wait Your Turn', 'It is not your turn yet.', EMBED_COLORS.info)], ephemeral: true });
    if (battle.critsLeft[interaction.user.id] <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Critical Left', 'You have no critical strikes left in this battle.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const enemyId = battle.players.find(id => id !== interaction.user.id);
    const attacker = await getUser(interaction.user.id);
    const defender = await getUser(enemyId);
    battle.critsLeft[interaction.user.id] -= 1;
    const result = resolveBattleDamage(attacker, defender, battle, 'critical');
    battle.turn = enemyId;

    if (battle.hp[enemyId] <= 0) {
      user.wins++;
      defender.losses++;
      const auraResult = addAura(user, 10000, 'aura');
      const xpResult = addXp(user, 150);
      const clanXpResult = await addClanXp(user.clan, 75);
      xpResult.level = user.level;

      await user.save();
      await defender.save();
      battles.delete(interaction.user.id);
      battles.delete(enemyId);

      return interaction.update({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.success)
            .setTitle('Critical Victory')
            .setDescription(`${result.dodged ? 'Enemy dodged the strike.' : `${result.damage}${result.crit ? ' damage (CRIT)' : ' damage'}`}\n+${auraResult.reward} Aura${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}${formatProgressExtras(xpResult)}${formatClanProgressExtras(clanXpResult)}`)
            .setTimestamp()
        ],
        components: [buildBattleRow(true)]
      });
    }

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle('Critical Strike')
          .setDescription(result.dodged ? 'Enemy dodged.' : `${result.damage}${result.crit ? ' damage (CRIT)' : ' damage'}`)
          .addFields(
            field('Crits Left', battle.critsLeft[interaction.user.id]),
            field('Next Turn', `<@${enemyId}>`),
            field('Your HP', `${battle.hp[interaction.user.id]}/${battleMaxHp(attacker)} ${bar(battle.hp[interaction.user.id], battleMaxHp(attacker))}`, false),
            field('Enemy HP', `${Math.max(battle.hp[enemyId], 0)}/${battleMaxHp(defender)} ${bar(Math.max(battle.hp[enemyId], 0), battleMaxHp(defender))}`, false)
          )
          .setTimestamp()
      ],
      components: [buildBattleRow(false)]
    });
  }
});

// ================= EXPRESS WEB =================
const app = express();
app.get('/', (req, res) => res.send('Bot running'));
app.get('/api/leaderboard', async (req, res) => {
  const top = await User.find().sort({ aura: -1 }).limit(10);
  res.json(top);
});
app.listen(3000, () => console.log('Web API running on port 3000'));

// ================= READY =================
client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
});

client.login(process.env.TOKEN);

