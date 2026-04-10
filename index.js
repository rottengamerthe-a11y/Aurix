require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { Client, GatewayIntentBits, ActionRowBuilder, ButtonBuilder, ButtonStyle, EmbedBuilder, PermissionsBitField, ModalBuilder, TextInputBuilder, TextInputStyle, AttachmentBuilder } = require('discord.js');
const { Resvg } = require('@resvg/resvg-js');
const mongoose = require('mongoose');
const express = require('express');

function getEnvValue(...keys) {
  for (const key of keys) {
    const value = process.env[key];
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return '';
}

const DISCORD_TOKEN = getEnvValue('DISCORD_TOKEN', 'TOKEN', 'BOT_TOKEN');
const MONGO_URI = getEnvValue('MONGO_URI', 'MONGODB_URI', 'DATABASE_URL');
const PORT = Number(process.env.PORT) || 3000;
const PUBLIC_BASE_URL = getEnvValue('PUBLIC_BASE_URL', 'RENDER_EXTERNAL_URL').replace(/\/+$/, '');
const missingEnvVars = [];
const BUILD_MARKER = 'visual-debug-2026-04-10-1';
const DEPLOY_COMMIT = getEnvValue('RENDER_GIT_COMMIT', 'RENDER_GIT_BRANCH') || 'local';
const configuredTokenVars = ['DISCORD_TOKEN', 'TOKEN', 'BOT_TOKEN']
  .map((key) => ({ key, value: getEnvValue(key) }))
  .filter((entry) => entry.value);
const configuredMongoVars = ['MONGO_URI', 'MONGODB_URI', 'DATABASE_URL']
  .map((key) => ({ key, value: getEnvValue(key) }))
  .filter((entry) => entry.value);

if (!DISCORD_TOKEN) missingEnvVars.push('TOKEN or DISCORD_TOKEN');
if (!MONGO_URI) missingEnvVars.push('MONGO_URI or MONGODB_URI');

if (missingEnvVars.length > 0) {
  console.error(`Missing required environment variables: ${missingEnvVars.join(', ')}`);
  console.error('Set them either in a local .env file or in your Render Environment settings.');
  process.exit(1);
}

if (configuredTokenVars.length > 1) {
  const uniqueTokenValues = new Set(configuredTokenVars.map((entry) => entry.value));
  if (uniqueTokenValues.size > 1) {
    console.warn(`Multiple Discord token env vars are set with different values: ${configuredTokenVars.map((entry) => entry.key).join(', ')}. Using DISCORD_TOKEN priority.`);
  }
}

if (configuredMongoVars.length > 1) {
  const uniqueMongoValues = new Set(configuredMongoVars.map((entry) => entry.value));
  if (uniqueMongoValues.size > 1) {
    console.warn(`Multiple Mongo env vars are set with different values: ${configuredMongoVars.map((entry) => entry.key).join(', ')}. Using MONGO_URI priority.`);
  }
}

console.log(`[boot] build=${BUILD_MARKER} deploy=${DEPLOY_COMMIT}`);
console.log(`[boot] publicBaseUrl=${PUBLIC_BASE_URL || 'not-set'}`);
if (PUBLIC_BASE_URL) {
  console.log(`[boot] visualTest.banner=${PUBLIC_BASE_URL}/visuals/local/core_profile/banner.png`);
  console.log(`[boot] visualTest.thumb=${PUBLIC_BASE_URL}/visuals/local/core_profile/thumb.png`);
}

process.on('unhandledRejection', (error) => {
  console.error('Unhandled promise rejection:', error);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
});

process.on('warning', (warning) => {
  console.warn('Process warning:', warning);
});

const parsedDiscordLoginTimeoutMs = Number(process.env.DISCORD_LOGIN_TIMEOUT_MS);
const DISCORD_LOGIN_TIMEOUT_MS = Number.isFinite(parsedDiscordLoginTimeoutMs)
  ? Math.max(0, parsedDiscordLoginTimeoutMs)
  : 0;
let discordReady = false;
let discordLoginWatchdog = null;

function scheduleDiscordLoginWatchdog() {
  clearTimeout(discordLoginWatchdog);
  if (DISCORD_LOGIN_TIMEOUT_MS === 0) {
    console.log('Discord login watchdog disabled (`DISCORD_LOGIN_TIMEOUT_MS=0`). Waiting indefinitely for Discord ready.');
    return;
  }
  discordLoginWatchdog = setTimeout(() => {
    if (discordReady) return;
    console.error(`Discord login timed out after ${DISCORD_LOGIN_TIMEOUT_MS}ms. Exiting so the host can restart the bot.`);
    process.exit(1);
  }, DISCORD_LOGIN_TIMEOUT_MS);
}

// ================= DATABASE =================
mongoose.connect(MONGO_URI)
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
  clanPrivacy: { type: String, default: 'private' },
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

const guildConfigSchema = new mongoose.Schema({
  guildId: { type: String, required: true, unique: true },
  botChannelId: { type: String, default: null }
});

const GuildConfig = mongoose.model('GuildConfig', guildConfigSchema);

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
  if (user.clanPrivacy !== 'public' && user.clanPrivacy !== 'private') user.clanPrivacy = 'private';
  if (!Number.isInteger(user.lastVaultInterest) || user.lastVaultInterest < 0) {
    user.lastVaultInterest = Date.now();
  }

  return user;
}

async function getGuildConfig(guildId) {
  let config = await GuildConfig.findOne({ guildId });
  if (!config) {
    config = await GuildConfig.create({ guildId });
  }

  return config;
}

async function ensureValidBotChannel(guild, guildConfig) {
  if (!guildConfig?.botChannelId) {
    return { config: guildConfig, reset: false };
  }

  let channel = guild.channels.cache.get(guildConfig.botChannelId) || null;

  if (!channel) {
    try {
      channel = await guild.channels.fetch(guildConfig.botChannelId);
    } catch (error) {
      channel = null;
    }
  }

  const canUseChannel = Boolean(
    channel &&
    channel.isTextBased?.() &&
    channel.permissionsFor(guild.members.me)?.has(PermissionsBitField.Flags.SendMessages)
  );

  if (canUseChannel) {
    return { config: guildConfig, reset: false };
  }

  guildConfig.botChannelId = null;
  await guildConfig.save();
  console.warn(`Reset invalid bot channel for guild ${guild.id}.`);
  return { config: guildConfig, reset: true };
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
let pendingClanInvites = new Map();
let pendingClanWars = new Map();
let activeClanWars = new Map();
let activeBossBattles = new Map();
let activeClanBossRaids = new Map();
let loggedMissingMessageContentWarning = false;

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
  { level: 1, reward: 'Clan unlocked. Member cap 50.', perks: { memberCap: 50, auraBonus: 0, xpBonus: 0, dailyBonus: 0, vaultBonus: 0 } },
  { level: 2, reward: '+2% clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.02, xpBonus: 0, dailyBonus: 0, vaultBonus: 0 } },
  { level: 3, reward: '+5% clan XP gain.', perks: { memberCap: 50, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0, vaultBonus: 0 } },
  { level: 4, reward: '+0.25% clan vault interest.', perks: { memberCap: 50, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0, vaultBonus: 0.0025 } },
  { level: 5, reward: '+5% clan daily reward.', perks: { memberCap: 50, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 6, reward: '+2% clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.02, xpBonus: 0.05, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 7, reward: '+4% total clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.04, xpBonus: 0.05, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 8, reward: '+10% total clan XP gain.', perks: { memberCap: 50, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.05, vaultBonus: 0.0025 } },
  { level: 9, reward: '+0.5% total clan vault interest.', perks: { memberCap: 50, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.05, vaultBonus: 0.005 } },
  { level: 10, reward: '+10% total clan daily reward.', perks: { memberCap: 50, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 11, reward: '+4% total clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.04, xpBonus: 0.1, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 12, reward: '+6% total clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.06, xpBonus: 0.1, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 13, reward: '+15% total clan XP gain.', perks: { memberCap: 50, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.1, vaultBonus: 0.005 } },
  { level: 14, reward: '+0.75% total clan vault interest.', perks: { memberCap: 50, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.1, vaultBonus: 0.0075 } },
  { level: 15, reward: '+15% total clan daily reward.', perks: { memberCap: 50, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 16, reward: '+6% total clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.06, xpBonus: 0.15, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 17, reward: '+8% total clan Aura gain.', perks: { memberCap: 50, auraBonus: 0.08, xpBonus: 0.15, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 18, reward: '+20% total clan XP gain.', perks: { memberCap: 50, auraBonus: 0.08, xpBonus: 0.2, dailyBonus: 0.15, vaultBonus: 0.0075 } },
  { level: 19, reward: '+1% total clan vault interest.', perks: { memberCap: 50, auraBonus: 0.08, xpBonus: 0.2, dailyBonus: 0.15, vaultBonus: 0.01 } },
  { level: 20, reward: 'Final clan rank: member cap 50, +10% Aura, +25% XP, +20% Daily, +1.25% vault interest.', perks: { memberCap: 50, auraBonus: 0.1, xpBonus: 0.25, dailyBonus: 0.2, vaultBonus: 0.0125 } }
];

const BOOST_LABELS = {
  aura: 'Aura Boost',
  daily: 'Daily Boost',
  crate: 'Luck Boost',
  xp: 'XP Boost'
};

const SHOP_ITEMS = {
  crate: {
    emoji: '📦',
    name: 'Crate',
    price: 2000,
    maxOwned: 25,
    description: 'Open it with !open for a random Aura reward.'
  },
  auraboost: {
    emoji: '🔥',
    name: 'Aura Boost',
    price: 15000,
    boostKey: 'aura',
    multiplier: 2,
    durationMs: 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use auraboost to double Aura rewards for 1 hour.'
  },
  dailyboost: {
    emoji: '🌞',
    name: 'Daily Boost',
    price: 20000,
    boostKey: 'daily',
    multiplier: 2,
    durationMs: 24 * 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use dailyboost to double daily rewards for 24 hours.'
  },
  luckboost: {
    emoji: '🍀',
    name: 'Luck Boost',
    price: 12000,
    boostKey: 'crate',
    multiplier: 1.5,
    durationMs: 60 * 60 * 1000,
    maxOwned: 1,
    description: 'Use !use luckboost to increase crate rewards by 50% for 1 hour.'
  },
  xpboost: {
    emoji: '✨',
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
  { name: '!setup [#channel]', description: 'Admins set the only channel this bot will respond in. Defaults to the current channel.' },
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
  { name: '!clan rename <new name>', description: 'Clan owner changes the clan name when there are no active wars or raids.' },
  { name: '!clan privacy <public|private>', description: 'Clan owner changes whether anyone can join or only invited users can join.' },
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

const VISUAL_ASSET_DIR = path.join(__dirname, 'assets', 'visuals');
const GENERATED_VISUAL_DIR = path.join(__dirname, 'assets', 'generated');
const renderedVisualCache = new Map();
const STATIC_PUBLIC_VISUALS = {
  core_profile: {
    banner: 'core-profile-banner.png',
    thumb: 'core-profile-thumb.png'
  },
  core_arcade: {
    banner: 'core-arcade-banner.png',
    thumb: 'core-arcade-thumb.png'
  }
};
const LOCAL_VISUALS = {
  help_summary: 'help-summary.svg',
  help_core: 'help-core.svg',
  core_arcade: 'core-arcade.svg',
  core_profile: 'core-profile.svg',
  economy_vault: 'economy-vault.svg',
  help_skills: 'help-skills.svg',
  help_clans: 'help-clans.svg',
  help_bosses: 'help-bosses.svg',
  clan_hall: 'clan-hall.svg',
  clan_war: 'clan-war.svg',
  clan_top: 'clan-top.svg',
  pvp_challenge: 'pvp-challenge.svg',
  pvp_battle: 'pvp-battle.svg',
  pvp_victory: 'pvp-victory.svg',
  boss_codex: 'boss-codex.svg',
  boss_ember: 'boss-ember.svg',
  boss_warden: 'boss-warden.svg',
  boss_oracle: 'boss-oracle.svg'
};

const LOCAL_VISUAL_THUMBNAILS = {
  help_summary: 'emblem-help.svg',
  help_core: 'emblem-help.svg',
  core_arcade: 'emblem-core-arcade.svg',
  core_profile: 'emblem-core-profile.svg',
  economy_vault: 'emblem-economy-vault.svg',
  help_skills: 'emblem-help.svg',
  help_clans: 'emblem-clan.svg',
  help_bosses: 'emblem-boss.svg',
  clan_hall: 'emblem-clan.svg',
  clan_war: 'emblem-clan.svg',
  clan_top: 'emblem-clan.svg',
  pvp_challenge: 'emblem-pvp.svg',
  pvp_battle: 'emblem-pvp.svg',
  pvp_victory: 'emblem-pvp.svg',
  boss_codex: 'emblem-boss.svg',
  boss_ember: 'emblem-boss.svg',
  boss_warden: 'emblem-boss.svg',
  boss_oracle: 'emblem-boss.svg'
};

const THEME_EMBLEM_FILES = {
  default: 'emblem-default.svg',
  help: 'emblem-help.svg',
  core: 'emblem-core.svg',
  economy: 'emblem-economy.svg',
  skills: 'emblem-skills.svg',
  clans: 'emblem-clan.svg',
  bosses: 'emblem-boss.svg',
  pvp: 'emblem-pvp.svg',
  success: 'emblem-success.svg',
  alert: 'emblem-alert.svg'
};

const THEME_BANNER_FILES = {
  default: 'help-summary.svg',
  help: 'help-summary.svg',
  core: 'help-core.svg',
  economy: 'help-core.svg',
  skills: 'help-skills.svg',
  clans: 'clan-hall.svg',
  bosses: 'boss-codex.svg',
  pvp: 'pvp-battle.svg',
  success: 'pvp-victory.svg',
  alert: 'help-bosses.svg'
};

const VISUAL_THEME_LIBRARY = {
  default: {
    thumbnail: process.env.VISUAL_DEFAULT_THUMBNAIL || null,
    banner: process.env.VISUAL_DEFAULT_BANNER || null
  },
  help: {
    thumbnail: process.env.VISUAL_HELP_THUMBNAIL || null,
    banner: process.env.VISUAL_HELP_BANNER || null,
    animation: process.env.VISUAL_HELP_ANIMATION || null
  },
  core: {
    thumbnail: process.env.VISUAL_CORE_THUMBNAIL || null,
    banner: process.env.VISUAL_CORE_BANNER || null,
    animation: process.env.VISUAL_CORE_ANIMATION || null
  },
  economy: {
    thumbnail: process.env.VISUAL_ECONOMY_THUMBNAIL || null,
    banner: process.env.VISUAL_ECONOMY_BANNER || null,
    animation: process.env.VISUAL_ECONOMY_ANIMATION || null
  },
  skills: {
    thumbnail: process.env.VISUAL_SKILLS_THUMBNAIL || null,
    banner: process.env.VISUAL_SKILLS_BANNER || null,
    animation: process.env.VISUAL_SKILLS_ANIMATION || null
  },
  clans: {
    thumbnail: process.env.VISUAL_CLANS_THUMBNAIL || null,
    banner: process.env.VISUAL_CLANS_BANNER || null,
    animation: process.env.VISUAL_CLANS_ANIMATION || null
  },
  bosses: {
    thumbnail: process.env.VISUAL_BOSSES_THUMBNAIL || null,
    banner: process.env.VISUAL_BOSSES_BANNER || null,
    animation: process.env.VISUAL_BOSSES_ANIMATION || null
  },
  pvp: {
    thumbnail: process.env.VISUAL_PVP_THUMBNAIL || null,
    banner: process.env.VISUAL_PVP_BANNER || null,
    animation: process.env.VISUAL_PVP_ANIMATION || null
  },
  success: {
    thumbnail: process.env.VISUAL_SUCCESS_THUMBNAIL || null,
    banner: process.env.VISUAL_SUCCESS_BANNER || null,
    animation: process.env.VISUAL_SUCCESS_ANIMATION || null
  },
  alert: {
    thumbnail: process.env.VISUAL_ALERT_THUMBNAIL || null,
    banner: process.env.VISUAL_ALERT_BANNER || null,
    animation: process.env.VISUAL_ALERT_ANIMATION || null
  }
};

function pickVisualTheme(title = '', description = '') {
  const content = `${title} ${description}`.toLowerCase();

  if (content.includes('help')) return 'help';
  if (content.includes('boss') || content.includes('raid')) return 'bosses';
  if (content.includes('clan')) return 'clans';
  if (content.includes('spin') || content.includes('coinflip') || content.includes('balance') || content.includes('rank') || content.includes('level') || content.includes('leaderboard') || content.includes('inventory') || content.includes('stats') || content.includes('profile')) return 'core';
  if (content.includes('skill') || content.includes('shop') || content.includes('crate') || content.includes('boost')) return 'skills';
  if (content.includes('pvp') || content.includes('duel') || content.includes('battle')) return 'pvp';
  if (content.includes('warning') || content.includes('invalid') || content.includes('cooldown') || content.includes('alert')) return 'alert';
  if (content.includes('daily') || content.includes('vault') || content.includes('interest') || content.includes('deposit')) return 'economy';
  if (content.includes('complete') || content.includes('joined') || content.includes('accepted') || content.includes('activated') || content.includes('victory') || content.includes('won') || content.includes('reward')) return 'success';

  return 'default';
}

function applyEmbedVisuals(embed, { title = '', description = '', theme = null, animated = true } = {}) {
  const selectedTheme = VISUAL_THEME_LIBRARY[theme || pickVisualTheme(title, description)] || VISUAL_THEME_LIBRARY.default;
  const heroImage = animated ? (selectedTheme.animation || selectedTheme.banner) : selectedTheme.banner;

  if (selectedTheme.thumbnail) {
    embed.setThumbnail(selectedTheme.thumbnail);
  }

  if (heroImage) {
    embed.setImage(heroImage);
  }

  return embed;
}

function getVisualCacheVersion(filePath) {
  return fs.statSync(filePath).mtimeMs.toString(36).replace('.', '');
}

function getLocalVisualUrlVersion(key) {
  const filename = LOCAL_VISUALS[key];
  if (!filename) return '0';

  const filePath = path.join(VISUAL_ASSET_DIR, filename);
  const thumbnailFilename = LOCAL_VISUAL_THUMBNAILS[key] || filename;
  const thumbnailPath = path.join(VISUAL_ASSET_DIR, thumbnailFilename);
  const bannerVersion = fs.existsSync(filePath) ? getVisualCacheVersion(filePath) : '0';
  const thumbVersion = fs.existsSync(thumbnailPath) ? getVisualCacheVersion(thumbnailPath) : bannerVersion;

  return `${bannerVersion}-${thumbVersion}`;
}

function getCachedRenderedPng(cacheKey, filePath, width) {
  const version = getVisualCacheVersion(filePath);
  const cached = renderedVisualCache.get(cacheKey);

  if (cached?.version === version) {
    return { buffer: cached.buffer, version };
  }

  const svg = fs.readFileSync(filePath, 'utf8');
  const buffer = new Resvg(svg, {
    fitTo: {
      mode: 'width',
      value: width
    }
  }).render().asPng();

  renderedVisualCache.set(cacheKey, { version, buffer });
  return { buffer, version };
}

function getLocalVisualAttachment(key) {
  const filename = LOCAL_VISUALS[key];
  if (!filename) return null;

  const filePath = path.join(VISUAL_ASSET_DIR, filename);
  if (!fs.existsSync(filePath)) return null;
  try {
    const thumbnailFilename = LOCAL_VISUAL_THUMBNAILS[key] || filename;
    const thumbnailPath = path.join(VISUAL_ASSET_DIR, thumbnailFilename);
    const { buffer: png, version } = getCachedRenderedPng(`local:${filename}:1200`, filePath, 1200);
    const thumbnailSourcePath = fs.existsSync(thumbnailPath) ? thumbnailPath : filePath;
    const { buffer: thumbnailPng, version: thumbnailVersion } = getCachedRenderedPng(
      `local:${thumbnailFilename}:256`,
      thumbnailSourcePath,
      256
    );
    const pngName = `${filename.replace(/\.svg$/i, '')}-${version}.png`;
    const thumbnailName = `thumb-${thumbnailFilename.replace(/\.svg$/i, '')}-${thumbnailVersion}.png`;

    return {
      name: pngName,
      thumbnailName,
      file: new AttachmentBuilder(png, { name: pngName }),
      thumbnailFile: new AttachmentBuilder(thumbnailPng, { name: thumbnailName })
    };
  } catch (error) {
    console.error(`Failed to render local visual "${key}":`, error);
    return null;
  }
}

function getThemeEmblemAttachment(theme = 'default') {
  const emblemFilename = THEME_EMBLEM_FILES[theme] || THEME_EMBLEM_FILES.default;
  const emblemPath = path.join(VISUAL_ASSET_DIR, emblemFilename);
  if (!fs.existsSync(emblemPath)) return null;
  try {
    const { buffer, version } = getCachedRenderedPng(`theme-emblem:${theme}:256`, emblemPath, 256);
    const pngName = `theme-${theme}-${version}.png`;

    return {
      name: pngName,
      file: new AttachmentBuilder(buffer, { name: pngName })
    };
  } catch (error) {
    console.error(`Failed to render theme emblem "${theme}":`, error);
    return null;
  }
}

function getThemeBannerAttachment(theme = 'default') {
  const bannerFilename = THEME_BANNER_FILES[theme] || THEME_BANNER_FILES.default;
  const bannerPath = path.join(VISUAL_ASSET_DIR, bannerFilename);
  if (!fs.existsSync(bannerPath)) return null;
  try {
    const { buffer, version } = getCachedRenderedPng(`theme-banner:${theme}:1200`, bannerPath, 1200);
    const pngName = `banner-${theme}-${version}.png`;

    return {
      name: pngName,
      file: new AttachmentBuilder(buffer, { name: pngName })
    };
  } catch (error) {
    console.error(`Failed to render theme banner "${theme}":`, error);
    return null;
  }
}

function runVisualSelfTest() {
  try {
    const emblem = getThemeEmblemAttachment('core');
    const banner = getThemeBannerAttachment('core');
    const local = getLocalVisualAttachment('core_profile');
    const economyLocal = getLocalVisualAttachment('economy_vault');

    console.log(
      `[visuals] core emblem=${Boolean(emblem)} banner=${Boolean(banner)} local=${Boolean(local)} economyLocal=${Boolean(economyLocal)}`
    );
  } catch (error) {
    console.error('[visuals] self-test failed:', error);
  }
}

async function runPublicVisualSelfTest() {
  if (!PUBLIC_BASE_URL || typeof fetch !== 'function') return;

  const targets = [
    `${PUBLIC_BASE_URL}/visuals/local/core_profile/banner.png`,
    `${PUBLIC_BASE_URL}/visuals/local/core_profile/thumb.png`
  ];

  for (const target of targets) {
    try {
      const response = await fetch(target, { method: 'GET' });
      console.log(
        `[visuals] public self-test url=${target} status=${response.status} contentType=${response.headers.get('content-type') || 'unknown'}`
      );
    } catch (error) {
      console.error(`[visuals] public self-test failed for ${target}:`, error);
    }
  }
}

function getPublicLocalVisualUrl(key, kind = 'banner') {
  if (!PUBLIC_BASE_URL) return null;
  const normalizedKind = kind === 'thumb' ? 'thumb' : 'banner';
  const staticVisual = STATIC_PUBLIC_VISUALS[key];

  if (staticVisual && staticVisual[normalizedKind]) {
    const staticPath = path.join(GENERATED_VISUAL_DIR, staticVisual[normalizedKind]);
    const version = fs.existsSync(staticPath) ? getVisualCacheVersion(staticPath) : '0';
    return `${PUBLIC_BASE_URL}/generated/${encodeURIComponent(staticVisual[normalizedKind])}?v=${encodeURIComponent(version)}`;
  }

  if (!LOCAL_VISUALS[key]) return null;
  const version = getLocalVisualUrlVersion(key);
  return `${PUBLIC_BASE_URL}/visuals/local/${encodeURIComponent(key)}/${normalizedKind}.png?v=${encodeURIComponent(version)}`;
}

function withLocalVisual(embed, key) {
  const publicBannerUrl = getPublicLocalVisualUrl(key, 'banner');
  const publicThumbUrl = getPublicLocalVisualUrl(key, 'thumb');
  if (publicBannerUrl && publicThumbUrl) {
    console.log(`[visuals] applying public visual key="${key}" banner="${publicBannerUrl}" thumbnail="${publicThumbUrl}" title="${embed.data?.title || ''}"`);
    embed.setImage(publicBannerUrl);
    embed.setThumbnail(publicThumbUrl);
    return { embed, files: [] };
  }

  const requestedAttachment = getLocalVisualAttachment(key);
  const attachment = requestedAttachment || (key === 'economy_vault' ? getLocalVisualAttachment('core_profile') : null);
  if (!attachment) {
    console.warn(`[visuals] local visual missing for key "${key}"`);
    return { embed, files: [] };
  }

  if (key === 'economy_vault' && !requestedAttachment) {
    console.warn('[visuals] falling back from "economy_vault" to "core_profile"');
  }

  const url = `attachment://${attachment.name}`;
  const thumbnailUrl = `attachment://${attachment.thumbnailName}`;
  console.log(`[visuals] applying local visual key="${key}" image="${attachment.name}" thumbnail="${attachment.thumbnailName}" title="${embed.data?.title || ''}"`);
  embed.setImage(url);
  embed.setThumbnail(thumbnailUrl);
  return { embed, files: [attachment.file, attachment.thumbnailFile] };
}

function visualReplyOptions(embed, key, extras = {}) {
  const { files } = withLocalVisual(embed, key);
  const payload = { embeds: [embed], ...extras, __skipVisualDecorate: true };

  if (files.length > 0) {
    payload.files = files;
  }

  return payload;
}

function normalizeReplyPayload(payload) {
  if (!payload) return payload;
  if (typeof payload === 'string') {
    const theme = pickVisualTheme('', payload);
    const embed = new EmbedBuilder()
      .setColor(theme === 'alert' ? EMBED_COLORS.danger : EMBED_COLORS.info)
      .setTitle('Aura Realms')
      .setDescription(payload)
      .setTimestamp();

    return { embeds: [applyEmbedVisuals(embed, { description: payload, theme })] };
  }
  if (Array.isArray(payload)) return payload;
  return { ...payload };
}

function decorateReplyPayload(payload) {
  const normalized = normalizeReplyPayload(payload);
  if (!normalized || typeof normalized === 'string' || Array.isArray(normalized)) return normalized;
  if (normalized.__skipVisualDecorate) {
    const { __skipVisualDecorate, ...rest } = normalized;
    return rest;
  }
  if (!Array.isArray(normalized.embeds) || normalized.embeds.length === 0) return normalized;
  if (Array.isArray(normalized.files) && normalized.files.length > 0) return normalized;

  const files = [];

  for (const embed of normalized.embeds) {
    if (!embed) continue;

    const imageUrl = embed.data?.image?.url || '';
    const thumbnailUrl = embed.data?.thumbnail?.url || '';
    if (String(imageUrl).startsWith('attachment://') || String(thumbnailUrl).startsWith('attachment://')) {
      continue;
    }

    const title = embed.data?.title || '';
    const description = embed.data?.description || '';
    const localVisualKey = getLocalVisualKeyForEmbed(title, description);
    const localVisual = localVisualKey ? getLocalVisualAttachment(localVisualKey) : null;

    if (localVisual) {
      embed.setImage(`attachment://${localVisual.name}`);
      embed.setThumbnail(`attachment://${localVisual.thumbnailName}`);
      files.push(localVisual.file, localVisual.thumbnailFile);
      continue;
    }

    const theme = pickVisualTheme(title, description);
    const emblem = getThemeEmblemAttachment(theme);
    const banner = getThemeBannerAttachment(theme);
    if (!emblem && !banner) continue;

    if (emblem) {
      embed.setThumbnail(`attachment://${emblem.name}`);
      files.push(emblem.file);
    }

    if (banner) {
      embed.setImage(`attachment://${banner.name}`);
      files.push(banner.file);
    }
  }

  if (files.length === 0) return normalized;
  normalized.files = files;
  return normalized;
}

function wrapReplyMethod(target, methodName) {
  if (!target || typeof target[methodName] !== 'function') return;

  const wrappedFlag = `__aurix_wrapped_${methodName}`;
  if (target[wrappedFlag]) return;

  const originalMethod = target[methodName].bind(target);
  target[methodName] = async function wrappedReplyMethod(...args) {
    if (args.length > 0) {
      args[0] = decorateReplyPayload(args[0]);
    }

    return originalMethod(...args);
  };
  target[wrappedFlag] = true;
}

function getHelpVisualKey(sectionKey = '') {
  if (!sectionKey) return 'help_summary';
  return `help_${sectionKey}`;
}

function getBossVisualKey(boss) {
  return boss?.key ? `boss_${boss.key}` : 'boss_codex';
}

function getClanVisualKey(title = '') {
  const normalized = title.toLowerCase();

  if (normalized.includes('war')) return 'clan_war';
  if (normalized.includes('top')) return 'clan_top';
  return 'clan_hall';
}

function getPvpVisualKey(title = '') {
  const normalized = title.toLowerCase();

  if (normalized.includes('victory')) return 'pvp_victory';
  if (normalized.includes('challenge')) return 'pvp_challenge';
  return 'pvp_battle';
}

function getCoreVisualKey(title = '') {
  const normalized = title.toLowerCase();

  if (normalized.includes('spin') || normalized.includes('coinflip')) return 'core_arcade';
  return 'core_profile';
}

function getEconomyVisualKey(title = '') {
  const normalized = title.toLowerCase();

  if (
    normalized.includes('deposit') ||
    normalized.includes('daily') ||
    normalized.includes('vault') ||
    normalized.includes('shop') ||
    normalized.includes('purchase') ||
    normalized.includes('boost')
  ) {
    return 'economy_vault';
  }

  return 'core_profile';
}

function getLocalVisualKeyForEmbed(title = '', description = '') {
  const content = `${title} ${description}`.toLowerCase();

  if (content.includes('help')) {
    if (content.includes('skill')) return 'help_skills';
    if (content.includes('clan')) return 'help_clans';
    if (content.includes('boss')) return 'help_bosses';
    if (content.includes('core')) return 'help_core';
    return 'help_summary';
  }

  if (content.includes('ember')) return 'boss_ember';
  if (content.includes('warden')) return 'boss_warden';
  if (content.includes('oracle')) return 'boss_oracle';
  if (content.includes('boss') || content.includes('raid')) return 'boss_codex';

  if (content.includes('clan')) return getClanVisualKey(content);
  if (content.includes('pvp') || content.includes('duel') || content.includes('battle')) return getPvpVisualKey(content);

  if (
    content.includes('deposit') ||
    content.includes('daily') ||
    content.includes('vault') ||
    content.includes('shop') ||
    content.includes('purchase') ||
    content.includes('boost') ||
    content.includes('aura')
  ) {
    return getEconomyVisualKey(content);
  }

  if (
    content.includes('spin') ||
    content.includes('coinflip') ||
    content.includes('balance') ||
    content.includes('rank') ||
    content.includes('level') ||
    content.includes('leaderboard') ||
    content.includes('inventory') ||
    content.includes('stats') ||
    content.includes('profile')
  ) {
    return getCoreVisualKey(content);
  }

  return null;
}

function createEmbed(message, title, color = EMBED_COLORS.primary, options = {}) {
  const embed = new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setFooter({ text: `${message.author.username} • Aura Realms` })
    .setTimestamp();

  return applyEmbedVisuals(embed, { title, theme: options.theme, animated: options.animated !== false });
}

function createPlainEmbed(title, color = EMBED_COLORS.primary, description = '', footerText = '') {
  const embed = new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setTimestamp();

  if (description) embed.setDescription(description);
  if (footerText) embed.setFooter({ text: footerText });
  return embed;
}

function createPlainMessageEmbed(message, title, color = EMBED_COLORS.primary, description = '') {
  return createPlainEmbed(title, color, description, `${message.author.username} • Aura Realms`);
}

function createPlainInteractionEmbed(title, color = EMBED_COLORS.primary, description = '') {
  return createPlainEmbed(title, color, description, 'Aura Realms • Interaction');
}

function field(name, value, inline = true) {
  return { name, value: String(value), inline };
}

function combatEmbed(title, color, description, fields = [], options = {}) {
  const embed = new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setDescription(description)
    .setTimestamp();

  if (fields.length > 0) {
    embed.addFields(fields);
  }

  return applyEmbedVisuals(embed, { title, description, theme: options.theme, animated: options.animated !== false });
}

function warningEmbed(message, title, description) {
  return createEmbed(message, title, EMBED_COLORS.danger, { theme: 'alert' }).setDescription(description);
}

function infoEmbed(message, title, description) {
  return createEmbed(message, title, EMBED_COLORS.info).setDescription(description);
}

function interactionNoticeEmbed(title, description, color = EMBED_COLORS.info) {
  const theme = color === EMBED_COLORS.danger
    ? 'alert'
    : color === EMBED_COLORS.success
      ? 'success'
      : null;
  const embed = new EmbedBuilder()
    .setColor(color)
    .setTitle(title)
    .setDescription(description)
    .setFooter({ text: 'Aura Realms • Interaction' })
    .setTimestamp();

  return applyEmbedVisuals(embed, { title, description, theme });
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
    .map(([item, count]) => `${getItemDisplayName(item)} x${count}`)
    .join(', ');
}

function activeBoostSummary(user) {
  removeExpiredBoosts(user);

  const active = Object.entries(user.activeBoosts)
    .map(([key, boost]) => `${BOOST_LABELS[key] || key} x${boost.multiplier} (${formatDuration(boost.expiresAt - Date.now())} left)`);

  return active.length > 0 ? active.join(', ') : 'No active boosts';
}

function findShopItemByName(itemName) {
  return Object.values(SHOP_ITEMS).find(item => item.name === itemName) || null;
}

function getItemDisplayName(itemName) {
  const item = findShopItemByName(itemName);
  return item?.emoji ? `${item.emoji} ${item.name}` : itemName;
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

function encodeClanToken(name) {
  return encodeURIComponent(name);
}

function decodeClanToken(token) {
  return decodeURIComponent(token);
}

function buildClanInviteRow(clanName, targetId, disabled = false) {
  const clanToken = encodeClanToken(clanName);

  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`claninvite_accept:${clanToken}:${targetId}`)
      .setLabel('Accept Invite')
      .setStyle(ButtonStyle.Success)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId(`claninvite_decline:${clanToken}:${targetId}`)
      .setLabel('Decline Invite')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(disabled)
  );
}

function buildClanJoinRow(clanName, disabled = false) {
  const clanToken = encodeClanToken(clanName);

  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`clanjoin_public:${clanToken}`)
      .setLabel('Join Clan')
      .setStyle(ButtonStyle.Primary)
      .setDisabled(disabled)
  );
}

function buildClanPrivacyRow(clanName, ownerId, currentPrivacy) {
  const clanToken = encodeClanToken(clanName);

  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`clanprivacy_public:${clanToken}:${ownerId}`)
      .setLabel('Make Public')
      .setStyle(ButtonStyle.Success)
      .setDisabled(currentPrivacy === 'public'),
    new ButtonBuilder()
      .setCustomId(`clanprivacy_private:${clanToken}:${ownerId}`)
      .setLabel('Make Private')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(currentPrivacy === 'private')
  );
}

function buildClanLeaveRow(clanName, disabled = false) {
  const clanToken = encodeClanToken(clanName);

  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`clanleave:${clanToken}`)
      .setLabel('Leave Clan')
      .setStyle(ButtonStyle.Danger)
      .setDisabled(disabled)
  );
}

function getHelpCategories() {
  return [
    {
      key: 'core',
      name: 'Core',
      summary: 'Basic profile, economy, and utility commands.',
      commands: COMMAND_INFO.filter(command => ['!help', '!setup [#channel]', '!spin', '!coinflip <heads/tails> <amount>', '!bal', '!rank', '!level', '!leaderboard', '!inv', '!stats', '!deposit <amount|all>', '!daily', '!vaultinterest'].includes(command.name))
    },
    {
      key: 'skills',
      name: 'Skills & Shop',
      summary: 'Skill upgrades, specializations, shopping, and items.',
      commands: COMMAND_INFO.filter(command => command.name.startsWith('!skill') || ['!shop', '!buy <item>', '!use <item>', '!open'].includes(command.name))
    },
    {
      key: 'clans',
      name: 'Clans',
      summary: 'Clan creation, invites, settings, war, and info.',
      commands: COMMAND_INFO.filter(command => command.name.startsWith('!clan'))
    },
    {
      key: 'bosses',
      name: 'Bosses',
      summary: 'Solo boss fights and clan raids.',
      commands: COMMAND_INFO.filter(command => command.name.startsWith('!boss'))
    }
  ];
}

function buildHelpRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('help_core').setLabel('Core').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('help_skills').setLabel('Skills').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('help_clans').setLabel('Clans').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('help_bosses').setLabel('Bosses').setStyle(ButtonStyle.Secondary)
  );
}

function buildHelpSummaryEmbed(message) {
  const categories = getHelpCategories();

  return createEmbed(message, 'Help Center', EMBED_COLORS.info)
    .setDescription('Use `!help <section>` for a full list.\nSections: `core`, `skills`, `clans`, `bosses`.')
    .addFields(categories.map(category => field(category.name, `${category.summary}\nUse \`!help ${category.key}\``, false)));
}

function buildHelpSectionEmbed(message, section) {
  return createEmbed(message, `${section.name} Help`, EMBED_COLORS.info)
    .setDescription(section.summary)
    .addFields(
      field(
        'Commands',
        section.commands.map(command => `\`${command.name}\`\n${command.description}`).join('\n\n') || 'No commands available.',
        false
      )
    );
}

function buildShopRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('shop_buy:crate').setLabel('📦 Crate').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('shop_buy:auraboost').setLabel('🔥 Aura').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('shop_buy:dailyboost').setLabel('🌞 Daily').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('shop_buy:luckboost').setLabel('🍀 Luck').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('shop_buy:xpboost').setLabel('✨ XP').setStyle(ButtonStyle.Secondary)
  );
}

function buildInventoryRow(user) {
  const buttons = [];

  if (hasItem(user, 'Crate')) {
    buttons.push(new ButtonBuilder().setCustomId('item_open_crate').setLabel('📦 Open Crate').setStyle(ButtonStyle.Primary));
  }

  for (const [itemKey, item] of Object.entries(SHOP_ITEMS)) {
    if (!item.boostKey) continue;
    if (!hasItem(user, item.name)) continue;

    buttons.push(
      new ButtonBuilder()
        .setCustomId(`item_use:${itemKey}`)
        .setLabel(`Use ${item.emoji} ${item.name}`)
        .setStyle(ButtonStyle.Success)
        .setDisabled(isBoostActive(user, item.boostKey))
    );
  }

  if (buttons.length === 0) return null;
  return new ActionRowBuilder().addComponents(buttons.slice(0, 5));
}

function buildSkillUpgradeRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('skill_upgrade:dmg').setLabel('Upgrade Dmg').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('skill_upgrade:defense').setLabel('Upgrade Defense').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('skill_upgrade:luck').setLabel('Upgrade Luck').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('skill_paths').setLabel('View Paths').setStyle(ButtonStyle.Secondary)
  );
}

function buildSkillPathRow(branch) {
  const paths = Object.keys(SKILL_SPECIALIZATIONS[branch] || {});

  return new ActionRowBuilder().addComponents(
    paths.map(path =>
      new ButtonBuilder()
        .setCustomId(`skill_specialize:${branch}:${path}`)
        .setLabel(`${branch}:${path}`)
        .setStyle(ButtonStyle.Secondary)
    )
  );
}

function buildBossStartRow() {
  return new ActionRowBuilder().addComponents(
    BOSS_TEMPLATES.map(boss =>
      new ButtonBuilder()
        .setCustomId(`boss_start:${boss.key}`)
        .setLabel(`Start ${boss.key}`)
        .setStyle(ButtonStyle.Danger)
    )
  );
}

function buildSoloBossRow(disabled = false) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('boss_attack').setLabel('Attack').setStyle(ButtonStyle.Danger).setDisabled(disabled),
    new ButtonBuilder().setCustomId('boss_heal').setLabel('Heal').setStyle(ButtonStyle.Success).setDisabled(disabled),
    new ButtonBuilder().setCustomId('boss_status').setLabel('Status').setStyle(ButtonStyle.Primary).setDisabled(disabled)
  );
}

function buildClanBossRow(disabled = false) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('boss_clan_join').setLabel('Join Raid').setStyle(ButtonStyle.Primary).setDisabled(disabled),
    new ButtonBuilder().setCustomId('boss_clan_attack').setLabel('Attack').setStyle(ButtonStyle.Danger).setDisabled(disabled),
    new ButtonBuilder().setCustomId('boss_clan_heal').setLabel('Heal').setStyle(ButtonStyle.Success).setDisabled(disabled),
    new ButtonBuilder().setCustomId('boss_clan_status').setLabel('Status').setStyle(ButtonStyle.Secondary).setDisabled(disabled)
  );
}

function buildEconomyRow(user) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('econ_daily').setLabel('Claim Daily').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('econ_deposit_all').setLabel('Deposit All').setStyle(ButtonStyle.Primary).setDisabled(user.aura <= 0),
    new ButtonBuilder().setCustomId('econ_vaultinterest').setLabel('Vault Info').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('econ_bal').setLabel('Refresh Balance').setStyle(ButtonStyle.Secondary)
  );
}

function buildEconomyModalRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('econ_coinflip_modal').setLabel('Coinflip').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('econ_deposit_modal').setLabel('Custom Deposit').setStyle(ButtonStyle.Primary)
  );
}

function buildEconomyRows(user) {
  return [buildEconomyRow(user), buildEconomyModalRow()];
}

function buildClanWarRow(user, war) {
  const isOwner = user.userId === war.attackerOwnerId || user.userId === war.defenderOwnerId;

  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('clanwar_join').setLabel('Join Roster').setStyle(ButtonStyle.Primary).setDisabled(war.started),
    new ButtonBuilder().setCustomId('clanwar_leave').setLabel('Leave Roster').setStyle(ButtonStyle.Secondary).setDisabled(war.started),
    new ButtonBuilder().setCustomId('clanwar_status').setLabel('War Status').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('clanwar_start').setLabel('Start War').setStyle(ButtonStyle.Danger).setDisabled(war.started || !isOwner)
  );
}

function buildClanCreateRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('clan_create_modal').setLabel('Create Clan').setStyle(ButtonStyle.Success)
  );
}

function buildClanRenameRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('clan_rename_modal').setLabel('Rename Clan').setStyle(ButtonStyle.Primary)
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

async function syncClanSettings(clanName, updates) {
  await User.updateMany({ clan: clanName }, { $set: updates });
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
    privacy: members[0].clanPrivacy || 'private',
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

function hasPendingClanWar(clanName) {
  for (const war of pendingClanWars.values()) {
    if (war.attackerClan === clanName || war.defenderClan === clanName) {
      return true;
    }
  }

  return false;
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
  wrapReplyMethod(message, 'reply');
  if (message.author.bot) return;
  if (!message.guild) return;
  console.log(`[messageCreate] guild=${message.guild.id} channel=${message.channelId} author=${message.author.id} content=${JSON.stringify(message.content || '')}`);
  if (!message.content) {
    if (!loggedMissingMessageContentWarning) {
      loggedMissingMessageContentWarning = true;
      console.warn('Received a guild message with empty content. If prefix commands are not working, enable Message Content Intent for this bot in the Discord Developer Portal and redeploy.');
    }
    return;
  }

  try {
    const isSetupCommand = message.content.toLowerCase().startsWith('!setup');
    const { config: guildConfig, reset: botChannelReset } = await ensureValidBotChannel(
      message.guild,
      await getGuildConfig(message.guild.id)
    );

    if (isSetupCommand) {
      if (!message.member.permissions.has(PermissionsBitField.Flags.ManageGuild)) {
        return message.reply({ embeds: [warningEmbed(message, 'Missing Permission', 'You need the `Manage Server` permission to set the bot channel.')] });
      }

      const selectedChannel = message.mentions.channels.first() || message.channel;
      if (!selectedChannel.isTextBased() || !selectedChannel.permissionsFor(message.guild.members.me)?.has(PermissionsBitField.Flags.SendMessages)) {
        return message.reply({ embeds: [warningEmbed(message, 'Invalid Channel', 'Choose a text channel where I have permission to send messages.')] });
      }

      guildConfig.botChannelId = selectedChannel.id;
      await guildConfig.save();

      return message.reply({
        embeds: [
          createEmbed(message, 'Setup Complete', EMBED_COLORS.success)
            .setDescription(`Bot commands are now locked to <#${selectedChannel.id}>.\nUse \`!setup #channel\` again any time to move them.`)
        ]
      });
    }

    if (guildConfig.botChannelId && message.channelId !== guildConfig.botChannelId) {
      if (message.content.startsWith('!')) {
        console.warn(`[messageCreate] ignored wrong channel. configured=${guildConfig.botChannelId} actual=${message.channelId}`);
        return message.reply({
          embeds: [
            warningEmbed(
              message,
              'Wrong Channel',
              `Use bot commands in <#${guildConfig.botChannelId}> or run \`!setup\` there to move the bot channel.`
            )
          ]
        });
      }
      return;
    }

    if (botChannelReset && message.content.startsWith('!')) {
      await message.reply({
        embeds: [
          createEmbed(message, 'Bot Channel Reset', EMBED_COLORS.info)
            .setDescription('The previously configured bot channel was missing or no longer accessible, so the lock was cleared. Run `!setup #channel` to choose a new bot channel.')
        ]
      });
    }

    const user = await getUser(message.author.id);
    removeExpiredBoosts(user);
    const vaultInterest = applyVaultInterest(user);
    if (vaultInterest.applied > 0) {
      await user.save();
    }

  if (message.content.startsWith('!help')) {
    const helpArg = (message.content.split(' ')[1] || '').toLowerCase();
    const categories = getHelpCategories();
    const selectedSection = categories.find(category => category.key === helpArg);

    if (!helpArg) {
      return message.reply(visualReplyOptions(buildHelpSummaryEmbed(message), getHelpVisualKey(), { components: [buildHelpRow()] }));
    }

    if (!selectedSection) {
      return message.reply({ embeds: [warningEmbed(message, 'Unknown Help Section', 'Use `!help`, `!help core`, `!help skills`, `!help clans`, or `!help bosses`.')] });
    }

    return message.reply(visualReplyOptions(buildHelpSectionEmbed(message, selectedSection), getHelpVisualKey(selectedSection.key), { components: [buildHelpRow()] }));
  }

  if (message.content === '!spin') {
    const cd = cooldown(user, 'spin', 300000);
    if (cd) {
      return message.reply(visualReplyOptions(
        createPlainMessageEmbed(message, 'Cooldown Active', EMBED_COLORS.danger, `Wait ${cd}s before using \`!spin\` again.`),
        getCoreVisualKey('Slot Spin')
      ));
    }

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
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title)));
  }

  if (message.content.startsWith('!coinflip')) {
    const cd = cooldown(user, 'cf', 120000);
    if (cd) {
      return message.reply(visualReplyOptions(
        createPlainMessageEmbed(message, 'Cooldown Active', EMBED_COLORS.danger, `Wait ${cd}s before using \`!coinflip\` again.`),
        getCoreVisualKey('Coinflip')
      ));
    }

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
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title)));
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

    return message.reply(visualReplyOptions(embed, getPvpVisualKey(embed.data.title), { components: [row] }));
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

    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title), { components: buildEconomyRows(user) }));
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
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title)));
  }

  if (message.content === '!level') {
    const embed = createEmbed(message, 'Level Progress', EMBED_COLORS.info)
      .addFields(
        field('Level', user.level),
        field('XP', `${user.xp}/${xpNeededForLevel(user.level)}`),
        field('Skill Points', availableSkillPoints(user)),
        field('Progress', levelProgress(user), false)
      );
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title)));
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
    return message.reply(visualReplyOptions(embed, 'help_skills', { components: [buildSkillUpgradeRow()] }));
  }

  if (message.content.startsWith('!skill')) {
    const args = message.content.split(' ');
    const action = (args[1] || '').toLowerCase();
    const skillName = (args[2] || '').toLowerCase();

    if (action === 'paths') {
      return message.reply(visualReplyOptions(
        infoEmbed(message, 'Skill Paths', `Skill specializations unlock at branch level 5.\n${specializationSummary()}`),
        'help_skills',
        {
        components: [buildSkillPathRow('dmg'), buildSkillPathRow('defense'), buildSkillPathRow('luck')]
        }
      ));
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
      return message.reply(visualReplyOptions(
        createEmbed(message, 'Specialization Chosen', EMBED_COLORS.success).setDescription(`Specialized **${skillName}** into **${pathName}**.\n${SKILL_SPECIALIZATIONS[skillName][pathName]}`),
        'help_skills',
        {
        components: [buildSkillUpgradeRow()]
        }
      ));
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
    return message.reply(visualReplyOptions(
      createEmbed(message, 'Skill Upgraded', EMBED_COLORS.success).setDescription(`Upgraded **${skillName}** to **${user.skills[skillName]}**.`).addFields(field('Tree Summary', skillTreeSummary(user), false)),
      'help_skills',
      {
      components: [buildSkillUpgradeRow()]
      }
    ));
  }

  if (message.content.startsWith('!deposit')) {
    const amountArg = message.content.split(' ')[1];
    if (!amountArg) {
      return message.reply(visualReplyOptions(
        createPlainMessageEmbed(message, 'Deposit Usage', EMBED_COLORS.info, 'Enter an amount to deposit.\nExample: `!deposit 5000` or `!deposit all`'),
        'core_profile'
      ));
    }

    const amount = amountArg.toLowerCase() === 'all' ? user.aura : parseInt(amountArg, 10);
    if (!Number.isInteger(amount) || amount <= 0) {
      return message.reply(visualReplyOptions(
        createPlainMessageEmbed(message, 'Invalid Amount', EMBED_COLORS.danger, 'Enter a valid deposit amount.'),
        'core_profile'
      ));
    }

    if (amount > user.aura) {
      return message.reply(visualReplyOptions(
        createPlainMessageEmbed(message, 'Not Enough Aura', EMBED_COLORS.danger, "You don't have that much Aura in your wallet."),
        'core_profile'
      ));
    }

    user.aura -= amount;
    user.vault += amount;
    user.lastVaultInterest = Date.now();
    updateRank(user);
    await user.save();
    const embed = createPlainMessageEmbed(message, 'Vault Deposit', EMBED_COLORS.success, `Deposited ${amount} Aura into your vault.`)
      .addFields(
        field('Wallet', user.aura),
        field('Vault', user.vault),
        field('Base Interest', `${Math.round(VAULT_INTEREST_RATE * 100)}% every 24h`)
      );
    return message.reply(visualReplyOptions(embed, 'core_profile', { components: buildEconomyRows(user) }));
  }

  if (message.content === '!daily') {
    const cd = cooldown(user, 'daily', 86400000);
    if (cd) {
      return message.reply(visualReplyOptions(
        createPlainMessageEmbed(message, 'Cooldown Active', EMBED_COLORS.danger, `Wait ${cd}s before claiming \`!daily\` again.`),
        'core_profile'
      ));
    }

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
    return message.reply(visualReplyOptions(embed, getEconomyVisualKey(embed.data.title), { components: buildEconomyRows(user) }));
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
    return message.reply(visualReplyOptions(embed, getEconomyVisualKey(embed.data.title), { components: buildEconomyRows(user) }));
  }

  if (message.content === '!shop') {
    const embed = createEmbed(message, 'Aura Shop', EMBED_COLORS.primary)
      .setDescription('Boost items are capped and cannot stack while active.')
      .addFields(
        Object.entries(SHOP_ITEMS).map(([key, item]) =>
          field(`${item.emoji} ${item.name} • ${item.price} Aura`, `Key: \`${key}\`\nLimit: ${item.maxOwned}\n${item.description}`, false)
        )
      );

    return message.reply(visualReplyOptions(embed, getEconomyVisualKey(embed.data.title), { components: [buildShopRow()] }));
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
      .setDescription(`Bought **${shopItem.emoji} ${shopItem.name}** for ${shopItem.price} Aura.`)
      .addFields(
        field('Wallet Left', user.aura),
        field('Owned', `${countItem(user, shopItem.name)}`),
        field('Limit', shopItem.maxOwned)
      );
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title)));
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
      .setDescription(`${shopItem.emoji} ${shopItem.name} is now active.`)
      .addFields(
        field('Effect', `${shopItem.multiplier}x ${BOOST_LABELS[shopItem.boostKey]}`),
        field('Duration', formatDuration(shopItem.durationMs))
      );
    return message.reply(visualReplyOptions(embed, getEconomyVisualKey(embed.data.title)));
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
      .setDescription('📦 Your crate burst open with a shower of loot.')
      .addFields(
        field('Aura', `+${auraResult.reward}${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}`),
        field('XP', `+${xpResult.reward}`),
        field('Clan XP', `+${clanXpResult.gained || 0}`)
      );
    return message.reply(visualReplyOptions(embed, 'core_arcade'));
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
      return message.reply(visualReplyOptions(embed, 'boss_codex', { components: [buildBossStartRow()] }));
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
      return message.reply(visualReplyOptions(embed, getBossVisualKey(boss), { components: [buildSoloBossRow()] }));
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
        return message.reply(visualReplyOptions(embed, getBossVisualKey(state.boss), { components: [buildBossStartRow()] }));
      }

      const counter = bossCounterDamage(user, state.boss);
      state.playerHp -= counter.damage;

      if (state.playerHp <= 0) {
        activeBossBattles.delete(user.userId);
        const embed = createEmbed(message, `Defeat: ${state.boss.name}`, EMBED_COLORS.danger)
          .setDescription(`The boss finished you with ${counter.dodged ? 'a missed counter' : `${counter.damage} damage`}.`)
          .addFields(field('Try Again', `Use \`!boss start ${state.boss.key}\``));
        return message.reply(visualReplyOptions(embed, getBossVisualKey(state.boss), { components: [buildBossStartRow()] }));
      }

      const embed = createEmbed(message, `Boss Turn: ${state.boss.name}`, EMBED_COLORS.danger)
        .setDescription(`You dealt ${hit.damage}${hit.crit ? ' (CRIT)' : ''}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
        .addFields(
          field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
          field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
          field('Heals Left', state.healsLeft)
        );
      return message.reply(visualReplyOptions(embed, getBossVisualKey(state.boss), { components: [buildSoloBossRow()] }));
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
        return message.reply(visualReplyOptions(embed, getBossVisualKey(state.boss), { components: [buildBossStartRow()] }));
      }

      const embed = createEmbed(message, `Boss Heal: ${state.boss.name}`, EMBED_COLORS.success)
        .setDescription(`You healed for ${healed}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
        .addFields(
          field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
          field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
          field('Heals Left', state.healsLeft)
        );
      return message.reply(visualReplyOptions(embed, getBossVisualKey(state.boss), { components: [buildSoloBossRow()] }));
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
      return message.reply(visualReplyOptions(embed, getBossVisualKey(state.boss), { components: [buildSoloBossRow()] }));
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
        return message.reply(visualReplyOptions(embed, getBossVisualKey(boss), { components: [buildClanBossRow()] }));
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
        return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildClanBossRow()] }));
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
        return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildClanBossRow()] }));
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
          return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildBossStartRow()] }));
        }

        const counter = bossCounterDamage(user, raid.boss);
        raid.playerHp[user.userId] -= counter.damage;
        raid.logs.push(counter.dodged ? `${message.author.username} dodged the counter.` : `${message.author.username} took ${counter.damage} counter damage.`);

        const livingMembers = raid.participants.filter(id => (raid.playerHp[id] || 0) > 0);
        if (livingMembers.length === 0) {
          activeClanBossRaids.delete(raid.clanName);
          const embed = createEmbed(message, `Raid Failed: ${raid.boss.name}`, EMBED_COLORS.danger)
            .setDescription(`The full **${raid.clanName}** roster was defeated.`);
          return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildBossStartRow()] }));
        }

        const embed = createEmbed(message, `Raid Turn: ${raid.boss.name}`, EMBED_COLORS.danger)
          .setDescription(`You dealt ${hit.damage}${hit.crit ? ' (CRIT)' : ''}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
          .addFields(
            field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
            field('Your HP', `${Math.max(raid.playerHp[user.userId], 0)}/${raid.playerMaxHp[user.userId]}`),
            field('Roster Alive', raid.participants.filter(id => (raid.playerHp[id] || 0) > 0).length)
          );
        return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildClanBossRow()] }));
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
          return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildBossStartRow()] }));
        }

        const embed = createEmbed(message, `Raid Heal: ${raid.boss.name}`, EMBED_COLORS.success)
          .setDescription(`You healed for ${healed}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
          .addFields(
            field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
            field('Your HP', `${Math.max(raid.playerHp[user.userId], 0)}/${raid.playerMaxHp[user.userId]}`),
            field('Heals Left', raid.healsLeft[user.userId])
          );
        return message.reply(visualReplyOptions(embed, getBossVisualKey(raid.boss), { components: [buildClanBossRow()] }));
      }

      return message.reply({ embeds: [infoEmbed(message, 'Clan Boss Commands', 'Use `!boss clan start <boss>`, `!boss clan join`, `!boss clan attack`, `!boss clan heal`, or `!boss clan status`.')] });
    }

    return message.reply({ embeds: [warningEmbed(message, 'Unknown Boss Command', 'Use `!boss list`, `!boss start <boss>`, `!boss attack`, `!boss heal`, `!boss status`, or `!boss clan ...`.')] });
  }

  if (message.content.startsWith('!clan')) {
    const args = message.content.split(' ').slice(1);
    const subcommand = (args[0] || '').toLowerCase();

    if (!subcommand) {
      return message.reply(visualReplyOptions(
        infoEmbed(message, 'Clan Commands', 'Use `!clan create <name>`, `!clan invite @user`, `!clan join <name>`, `!clan accept <name>`, `!clan decline <name>`, `!clan rename <new name>`, `!clan privacy <public|private>`, `!clan leave`, `!clan transfer @user`, `!clan kick @user`, `!clan war <name>`, `!clan info [name]`, or `!clan top`.'),
        getClanVisualKey('clan commands'),
        { components: !user.clan ? [buildClanCreateRow()] : user.clanRole === 'owner' ? [buildClanRenameRow()] : [] }
      ));
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
      user.clanPrivacy = 'private';
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
          field('Privacy', user.clanPrivacy),
          field('Member Cap', getClanLevelData(user.clanLevel).perks.memberCap),
          field('Wallet Left', user.aura)
        );
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
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
      pendingClanInvites.set(`${user.clan}:${targetUser.id}`, {
        clanName: user.clan,
        inviterId: user.userId,
        targetId: targetUser.id,
        createdAt: Date.now()
      });

      const embed = createEmbed(message, 'Clan Invite Sent', EMBED_COLORS.info)
        .setDescription(`Invited **${targetUser.username}** to **${user.clan}**.`)
        .addFields(field('Invitee', `<@${targetUser.id}>`), field('Privacy', user.clanPrivacy));
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: [buildClanInviteRow(user.clan, targetUser.id)] }));
    }

    if (subcommand === 'join') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply({ embeds: [infoEmbed(message, 'Clan Join Usage', 'Enter a clan name to join.\nExample: `!clan join Shadow Guild`')] });
      if (user.clan) return message.reply({ embeds: [warningEmbed(message, 'Already In Clan', `You are already in **${user.clan}**. Leave it first before joining another clan.`)] });

      const existingClan = await User.findOne({ clan: new RegExp(`^${clanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') });
      if (!existingClan) return message.reply({ embeds: [warningEmbed(message, 'Clan Not Found', 'That clan does not exist.')] });
      const hasInvite = user.clanInvites.includes(existingClan.clan);
      const isPublicClan = (existingClan.clanPrivacy || 'private') === 'public';
      if (!hasInvite && !isPublicClan) return message.reply({ embeds: [warningEmbed(message, 'Invite Required', `You need an invite to join **${existingClan.clan}**. Use \`!clan accept ${existingClan.clan}\` after being invited.`)] });
      const clanPerks = getClanLevelData(existingClan.clanLevel || 1).perks;
      const memberCount = await User.countDocuments({ clan: existingClan.clan });
      if (memberCount >= clanPerks.memberCap) return message.reply({ embeds: [warningEmbed(message, 'Clan Full', `**${existingClan.clan}** is full. Member cap: ${clanPerks.memberCap}.`)] });

      user.clan = existingClan.clan;
      user.clanRole = 'member';
      user.clanPrivacy = existingClan.clanPrivacy || 'private';
      user.clanInvites = user.clanInvites.filter(invite => invite !== existingClan.clan);
      user.clanLevel = existingClan.clanLevel || 1;
      user.clanXp = existingClan.clanXp || 0;
      await user.save();
      pendingClanInvites.delete(`${existingClan.clan}:${user.userId}`);
      const embed = createEmbed(message, 'Clan Joined', EMBED_COLORS.success)
        .setDescription(`You joined **${existingClan.clan}**.`)
        .addFields(
          field('Clan Level', user.clanLevel),
          field('Role', user.clanRole),
          field('Privacy', user.clanPrivacy)
        );
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    if (subcommand === 'accept') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply({ embeds: [infoEmbed(message, 'Clan Accept Usage', 'Enter a clan name to accept.\nExample: `!clan accept Shadow Guild`')] });
      if (user.clan) return message.reply({ embeds: [warningEmbed(message, 'Already In Clan', `You are already in **${user.clan}**.`)] });

      const inviteName = user.clanInvites.find(invite => invite.toLowerCase() === clanName.toLowerCase());
      if (!inviteName) return message.reply({ embeds: [warningEmbed(message, 'Invite Missing', 'You do not have an invite to that clan.')] });

      const existingClan = await User.findOne({ clan: inviteName });
      if (!existingClan) {
        user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
        await user.save();
        return message.reply({ embeds: [warningEmbed(message, 'Clan Missing', 'That clan no longer exists.')] });
      }

      const clanPerks = getClanLevelData(existingClan.clanLevel || 1).perks;
      const memberCount = await User.countDocuments({ clan: existingClan.clan });
      if (memberCount >= clanPerks.memberCap) return message.reply({ embeds: [warningEmbed(message, 'Clan Full', `**${existingClan.clan}** is full. Member cap: ${clanPerks.memberCap}.`)] });

      user.clan = existingClan.clan;
      user.clanRole = 'member';
      user.clanPrivacy = existingClan.clanPrivacy || 'private';
      user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
      user.clanLevel = existingClan.clanLevel || 1;
      user.clanXp = existingClan.clanXp || 0;
      await user.save();
      pendingClanInvites.delete(`${existingClan.clan}:${user.userId}`);
      const embed = createEmbed(message, 'Clan Invite Accepted', EMBED_COLORS.success)
        .setDescription(`You joined **${existingClan.clan}**.`)
        .addFields(
          field('Clan Level', user.clanLevel),
          field('Role', user.clanRole),
          field('Privacy', user.clanPrivacy)
        );
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    if (subcommand === 'decline') {
      const clanName = normalizeClanName(args.slice(1).join(' '));
      if (!clanName) return message.reply({ embeds: [infoEmbed(message, 'Clan Decline Usage', 'Enter a clan name to decline.\nExample: `!clan decline Shadow Guild`')] });

      const inviteName = user.clanInvites.find(invite => invite.toLowerCase() === clanName.toLowerCase());
      if (!inviteName) return message.reply({ embeds: [warningEmbed(message, 'Invite Missing', 'You do not have an invite to that clan.')] });

      user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
      await user.save();
      pendingClanInvites.delete(`${inviteName}:${user.userId}`);
      const embed = createEmbed(message, 'Clan Invite Declined', EMBED_COLORS.danger)
        .setDescription(`Declined invite to **${inviteName}**.`);
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    if (subcommand === 'rename') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You are not in a clan.')] });
      if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can rename the clan.')] });

      const newClanName = normalizeClanName(args.slice(1).join(' '));
      if (!newClanName) return message.reply({ embeds: [infoEmbed(message, 'Clan Rename Usage', 'Enter a new clan name.\nExample: `!clan rename Shadow Order`')] });
      if (!isValidClanName(newClanName)) return message.reply({ embeds: [warningEmbed(message, 'Invalid Clan Name', 'Clan names must be 3-20 characters and use only letters, numbers, and spaces.')] });
      if (newClanName.toLowerCase() === user.clan.toLowerCase()) return message.reply({ embeds: [warningEmbed(message, 'Same Clan Name', 'That is already your clan name.')] });
      if (getActiveClanWar(user.clan) || hasPendingClanWar(user.clan) || getActiveClanBossRaid(user.clan)) {
        return message.reply({ embeds: [warningEmbed(message, 'Clan Busy', 'Finish active clan wars, pending clan wars, and clan raids before renaming the clan.')] });
      }

      const oldClanName = user.clan;
      const existingClan = await User.findOne({ clan: new RegExp(`^${newClanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') });
      if (existingClan) return message.reply({ embeds: [warningEmbed(message, 'Clan Name Taken', 'That clan name is already taken.')] });

      await User.updateMany({ clan: oldClanName }, { $set: { clan: newClanName } });

      const invitedUsers = await User.find({ clanInvites: oldClanName });
      for (const invitedUser of invitedUsers) {
        invitedUser.clanInvites = invitedUser.clanInvites.map(invite => invite === oldClanName ? newClanName : invite);
        await invitedUser.save();
      }

      for (const [key, invite] of pendingClanInvites.entries()) {
        if (invite.clanName === oldClanName) {
          pendingClanInvites.delete(key);
          pendingClanInvites.set(`${newClanName}:${invite.targetId}`, { ...invite, clanName: newClanName });
        }
      }

      user.clan = newClanName;
      await user.save();

      return message.reply({
        ...visualReplyOptions(
          createEmbed(message, 'Clan Renamed', EMBED_COLORS.success)
            .setDescription(`Your clan is now **${newClanName}**.`)
            .addFields(field('Old Name', oldClanName), field('New Name', newClanName)),
          getClanVisualKey('clan renamed')
        )
      });
    }

    if (subcommand === 'privacy') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You are not in a clan.')] });
      if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can change clan privacy.')] });

      const mode = (args[1] || '').toLowerCase();
      if (!['public', 'private'].includes(mode)) {
        return message.reply({ embeds: [infoEmbed(message, 'Clan Privacy Usage', 'Choose `public` or `private`.\nExample: `!clan privacy public`')] });
      }

      await syncClanSettings(user.clan, { clanPrivacy: mode });
      user.clanPrivacy = mode;

      return message.reply({
        ...visualReplyOptions(
          createEmbed(message, 'Clan Privacy Updated', EMBED_COLORS.info)
            .setDescription(`**${user.clan}** is now **${mode}**.`)
            .addFields(field('Join Rule', mode === 'public' ? 'Anyone can join with `!clan join` or the Join Clan button.' : 'Only invited players can join.')),
          getClanVisualKey('clan privacy')
        )
      });
    }

    if (subcommand === 'leave') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You are not in a clan.')] });
      if (user.clanRole === 'owner') {
        const otherMembers = await User.countDocuments({ clan: user.clan, userId: { $ne: user.userId } });
        if (otherMembers > 0) {
          return message.reply({ embeds: [warningEmbed(message, 'Transfer Ownership First', 'Transfer ownership before leaving your clan.')] });
        }
      }

      const oldClan = user.clan;
      const oldMembers = await User.countDocuments({ clan: oldClan });
      user.clan = null;
      user.clanRole = null;
      user.clanPrivacy = 'private';
      user.clanLevel = 1;
      user.clanXp = 0;
      await user.save();
      if (oldMembers === 1) {
        const embed = createEmbed(message, 'Clan Left', EMBED_COLORS.danger)
          .setDescription(`You left **${oldClan}**. The clan now has no members.`);
        return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
      }
      const embed = createEmbed(message, 'Clan Left', EMBED_COLORS.danger)
        .setDescription(`You left **${oldClan}**.`);
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    if (subcommand === 'transfer') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You are not in a clan.')] });
      if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can transfer ownership.')] });

      const targetUser = message.mentions.users.first();
      if (!targetUser) return message.reply({ embeds: [infoEmbed(message, 'Clan Transfer Usage', 'Mention a clan member to transfer ownership to.')] });

      const target = await User.findOne({ userId: targetUser.id, clan: user.clan });
      if (!target) return message.reply({ embeds: [warningEmbed(message, 'Member Not Found', 'That user is not in your clan.')] });
      if (target.userId === user.userId) return message.reply({ embeds: [warningEmbed(message, 'Already Owner', 'You already own the clan.')] });

      user.clanRole = 'member';
      target.clanRole = 'owner';
      await user.save();
      await target.save();
      const embed = createEmbed(message, 'Ownership Transferred', EMBED_COLORS.info)
        .setDescription(`Transferred **${user.clan}** ownership to **${targetUser.username}**.`);
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    if (subcommand === 'kick') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You are not in a clan.')] });
      if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can kick members.')] });

      const targetUser = message.mentions.users.first();
      if (!targetUser) return message.reply({ embeds: [infoEmbed(message, 'Clan Kick Usage', 'Mention a clan member to kick.')] });

      const target = await User.findOne({ userId: targetUser.id, clan: user.clan });
      if (!target) return message.reply({ embeds: [warningEmbed(message, 'Member Not Found', 'That user is not in your clan.')] });
      if (target.clanRole === 'owner') return message.reply({ embeds: [warningEmbed(message, 'Invalid Target', 'You cannot kick the clan owner.')] });

      target.clan = null;
      target.clanRole = null;
      target.clanPrivacy = 'private';
      target.clanLevel = 1;
      target.clanXp = 0;
      await target.save();
      const embed = createEmbed(message, 'Member Kicked', EMBED_COLORS.danger)
        .setDescription(`Removed **${targetUser.username}** from **${user.clan}**.`);
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    if (subcommand === 'war') {
      if (!user.clan) return message.reply({ embeds: [warningEmbed(message, 'No Clan', 'You are not in a clan.')] });
      const warAction = (args[1] || '').toLowerCase();

      if (warAction === 'join') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply({ embeds: [warningEmbed(message, 'No Active War', 'Your clan does not have an active clan war.')] });
        if (war.started) return message.reply({ embeds: [warningEmbed(message, 'War Started', 'This clan war has already started.')] });

        const rosterKey = war.attackerClan === user.clan ? 'attackerParticipants' : 'defenderParticipants';
        const clanLevel = war.attackerClan === user.clan ? war.attackerLevel : war.defenderLevel;
        const cap = clanWarParticipantCap(clanLevel);
        if (war[rosterKey].includes(user.userId)) return message.reply({ embeds: [warningEmbed(message, 'Already Joined', 'You are already on the war roster.')] });
        if (war[rosterKey].length >= cap) return message.reply({ embeds: [warningEmbed(message, 'Roster Full', `Your clan war roster is full. Cap: ${cap}.`)] });

        war[rosterKey].push(user.userId);
        war.logs.push(`${message.author.username} joined the ${user.clan} war roster.`);
        const embed = createEmbed(message, 'Clan War Roster', EMBED_COLORS.info)
          .setDescription(`You joined the war roster for **${user.clan}**.`)
          .addFields(field('Roster Size', war[rosterKey].length), field('Cap', cap));
        return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: [buildClanWarRow(user, war)] }));
      }

      if (warAction === 'leave') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply({ embeds: [warningEmbed(message, 'No Active War', 'Your clan does not have an active clan war.')] });
        if (war.started) return message.reply({ embeds: [warningEmbed(message, 'War Started', 'This clan war has already started.')] });

        const rosterKey = war.attackerClan === user.clan ? 'attackerParticipants' : 'defenderParticipants';
        war[rosterKey] = war[rosterKey].filter(id => id !== user.userId);
        war.logs.push(`${message.author.username} left the ${user.clan} war roster.`);
        const embed = createEmbed(message, 'Clan War Roster', EMBED_COLORS.danger)
          .setDescription(`You left the war roster for **${user.clan}**.`);
        return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: [buildClanWarRow(user, war)] }));
      }

      if (warAction === 'status') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply({ embeds: [warningEmbed(message, 'No Active War', 'Your clan does not have an active clan war.')] });

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
        return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: [buildClanWarRow(user, war)] }));
      }

      if (warAction === 'start') {
        const war = getActiveClanWar(user.clan);
        if (!war) return message.reply({ embeds: [warningEmbed(message, 'No Active War', 'Your clan does not have an active clan war.')] });
        if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only clan owners can start clan wars.')] });
        if (war.started) return message.reply({ embeds: [warningEmbed(message, 'War Started', 'This clan war has already started.')] });
        if (user.userId !== war.attackerOwnerId && user.userId !== war.defenderOwnerId) {
          return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the two clan owners can start this war.')] });
        }

        if (war.attackerParticipants.length === 0 || war.defenderParticipants.length === 0) {
          return message.reply({ embeds: [warningEmbed(message, 'Need Rosters', 'Both clans need at least one participant before the war can start.')] });
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
        return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: [] }));
      }

      if (user.clanRole !== 'owner') return message.reply({ embeds: [warningEmbed(message, 'Owner Only', 'Only the clan owner can start a clan war.')] });
      if (getActiveClanWar(user.clan)) return message.reply({ embeds: [warningEmbed(message, 'War Already Active', 'Your clan already has an active clan war.')] });

      const targetClanName = normalizeClanName(args.slice(1).join(' '));
      if (!targetClanName) return message.reply({ embeds: [infoEmbed(message, 'Clan War Usage', 'Enter a clan name to challenge.\nExample: `!clan war Shadow Guild`')] });
      if (targetClanName.toLowerCase() === user.clan.toLowerCase()) return message.reply({ embeds: [warningEmbed(message, 'Invalid Target', 'You cannot challenge your own clan.')] });

      const targetSummary = await getClanSummary(targetClanName);
      if (!targetSummary) return message.reply({ embeds: [warningEmbed(message, 'Clan Not Found', 'That clan does not exist.')] });
      if (getActiveClanWar(targetSummary.name)) return message.reply({ embeds: [warningEmbed(message, 'War Already Active', 'That clan already has an active clan war.')] });

      const targetOwner = targetSummary.members.find(member => member.clanRole === 'owner');
      if (!targetOwner) return message.reply({ embeds: [warningEmbed(message, 'Owner Missing', 'That clan does not currently have an owner.')] });

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
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: [row] }));
    }

    if (subcommand === 'info') {
      const requestedName = normalizeClanName(args.slice(1).join(' '));
      const clanName = requestedName || user.clan;
      if (!clanName) return message.reply({ embeds: [infoEmbed(message, 'Clan Info Usage', 'Enter a clan name or join a clan first.\nExample: `!clan info Shadow Guild`')] });

      const summary = await getClanSummary(clanName);
      if (!summary) return message.reply({ embeds: [warningEmbed(message, 'Clan Not Found', 'That clan was not found.')] });
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
          field('Privacy', summary.privacy),
          field('Members', `${summary.members.length}/${clanData.perks.memberCap}`),
          field('Total Aura', summary.totalAura),
          field('Total Wins', summary.totalWins),
          field('Power', Math.floor(getClanPower(summary))),
          field('Clan Progress', clanLevelProgress(summary.members[0]), false),
          field('Roster', memberText || 'No members', false)
        );

      const rows = [];
      const hasInvite = user.clanInvites.includes(summary.name);
      if (!user.clan && summary.privacy === 'public') {
        rows.push(buildClanJoinRow(summary.name));
      } else if (!user.clan && hasInvite) {
        rows.push(buildClanInviteRow(summary.name, user.userId));
      } else if (user.clan === summary.name) {
        if (user.clanRole === 'owner') {
          rows.push(buildClanPrivacyRow(summary.name, user.userId, summary.privacy));
          rows.push(buildClanRenameRow());
        }
        rows.push(buildClanLeaveRow(summary.name));
      }

      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title), { components: rows }));
    }

    if (subcommand === 'top') {
      const clanUsers = await User.find({ clan: { $ne: null } }).sort({ clan: 1 });
      if (clanUsers.length === 0) return message.reply({ embeds: [infoEmbed(message, 'No Clans Yet', 'No clans exist yet.')] });

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
      return message.reply(visualReplyOptions(embed, getClanVisualKey(embed.data.title)));
    }

    return message.reply({ embeds: [warningEmbed(message, 'Unknown Clan Command', 'Use `!clan create`, `!clan invite`, `!clan join`, `!clan accept`, `!clan decline`, `!clan rename`, `!clan privacy`, `!clan leave`, `!clan transfer`, `!clan kick`, `!clan war <name>`, `!clan war join`, `!clan war leave`, `!clan war start`, `!clan war status`, `!clan info`, or `!clan top`.')] });
  }

  if (message.content === '!leaderboard') {
    const top = await User.find().sort({ aura: -1 }).limit(5);
    const text = top.map((u, i) => `${i + 1}. <@${u.userId}>: ${u.aura}`).join('\n');
    const embed = createEmbed(message, 'Top Players', EMBED_COLORS.primary)
      .setDescription(text || 'No players yet.');
    return message.reply(visualReplyOptions(embed, 'core_profile'));
  }

  if (message.content === '!inv') {
    const embed = createEmbed(message, 'Inventory', EMBED_COLORS.primary)
      .addFields(
        field('Items', inventorySummary(user), false),
        field('Active Boosts', activeBoostSummary(user), false)
      );
    const inventoryRow = buildInventoryRow(user);
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title), { components: inventoryRow ? [inventoryRow] : [] }));
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
    return message.reply(visualReplyOptions(embed, getCoreVisualKey(embed.data.title)));
  }
  } catch (error) {
    console.error(`messageCreate handler failed for guild ${message.guild?.id} channel ${message.channelId} user ${message.author?.id}:`, error);
    if (!message.channel) return;
    try {
      await message.reply({
        embeds: [
          warningEmbed(
            message,
            'Command Error',
            'Something went wrong while processing that command. Check the Render logs for the detailed error.'
          )
        ]
      });
    } catch (replyError) {
      console.error('Failed to send command error reply:', replyError);
    }
  }
});

// ================= BUTTON-BASED PVP =================
client.on('interactionCreate', async (interaction) => {
  wrapReplyMethod(interaction, 'reply');
  wrapReplyMethod(interaction, 'followUp');
  wrapReplyMethod(interaction, 'editReply');
  if (!interaction.isButton()) return;
  if (!interaction.guild) {
    return interaction.reply({ embeds: [interactionNoticeEmbed('Server Only', 'These buttons can only be used inside a server.', EMBED_COLORS.danger)], ephemeral: true });
  }

  const { config: guildConfig } = await ensureValidBotChannel(
    interaction.guild,
    await getGuildConfig(interaction.guild.id)
  );
  if (guildConfig.botChannelId && interaction.channelId !== guildConfig.botChannelId) {
    return interaction.reply({
      embeds: [
        interactionNoticeEmbed('Wrong Channel', `Use bot interactions in <#${guildConfig.botChannelId}>.`, EMBED_COLORS.danger)
      ],
      ephemeral: true
    });
  }

  const user = await getUser(interaction.user.id);
  removeExpiredBoosts(user);
  const vaultInterest = applyVaultInterest(user);
  if (vaultInterest.applied > 0) {
    await user.save();
  }

  if (interaction.customId.startsWith('help_')) {
    const sectionKey = interaction.customId.replace('help_', '');
    const section = getHelpCategories().find(category => category.key === sectionKey);

    if (!section) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Help Missing', 'That help section was not found.', EMBED_COLORS.danger)], ephemeral: true });
    }

    return interaction.reply({
      ...visualReplyOptions(buildHelpSectionEmbed({ author: interaction.user }, section), getHelpVisualKey(section.key), {
        components: [buildHelpRow()],
        ephemeral: true
      })
    });
  }

  if (interaction.customId.startsWith('shop_buy:')) {
    const itemKey = interaction.customId.split(':')[1];
    const shopItem = SHOP_ITEMS[itemKey];

    if (!shopItem) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Unknown Item', 'That item is not in the shop.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (user.aura < shopItem.price) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Not Enough Aura', 'You do not have enough Aura for that purchase.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const ownedCount = countItem(user, shopItem.name);
    if (ownedCount >= shopItem.maxOwned) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Item Cap Reached', `You already have the max allowed ${shopItem.name} (${shopItem.maxOwned}).`, EMBED_COLORS.danger)], ephemeral: true });
    }

    user.aura -= shopItem.price;
    user.inventory.push(shopItem.name);
    updateRank(user);
    await user.save();

    return interaction.reply(visualReplyOptions(
      interactionNoticeEmbed('Purchase Complete', `Bought **${shopItem.emoji} ${shopItem.name}** for ${shopItem.price} Aura.`, EMBED_COLORS.success),
      getEconomyVisualKey('Purchase Complete'),
      { ephemeral: true }
    ));
  }

  if (interaction.customId.startsWith('item_use:')) {
    const itemKey = interaction.customId.split(':')[1];
    const shopItem = SHOP_ITEMS[itemKey];

    if (!shopItem || !shopItem.boostKey) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Item Not Usable', 'That item cannot be used.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (!hasItem(user, shopItem.name)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Missing Item', `You do not have ${shopItem.name}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    if (isBoostActive(user, shopItem.boostKey)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Boost Already Active', `${BOOST_LABELS[shopItem.boostKey]} is already active.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    removeItem(user, shopItem.name);
    activateBoost(user, shopItem.boostKey, shopItem.multiplier, shopItem.durationMs);
    await user.save();

    return interaction.reply(visualReplyOptions(
      interactionNoticeEmbed('Boost Activated', `${shopItem.emoji} ${shopItem.name} is now active for ${formatDuration(shopItem.durationMs)}.`, EMBED_COLORS.success),
      getEconomyVisualKey('Boost Activated'),
      { ephemeral: true }
    ));
  }

  if (interaction.customId === 'item_open_crate') {
    if (!hasItem(user, 'Crate')) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Crate', 'You do not have a crate to open.', EMBED_COLORS.danger)], ephemeral: true });
    }

    removeItem(user, 'Crate');
    const auraResult = addAura(user, Math.floor(Math.random() * 8000), 'crate');
    const xpResult = addXp(user, 50 + Math.floor(Math.random() * 30));
    const clanXpResult = await addClanXp(user.clan, 25);
    xpResult.level = user.level;
    await user.save();

    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.primary)
          .setTitle('Crate Opened')
          .setDescription('📦 Your crate burst open with a shower of loot.')
          .addFields(
            field('Aura', `+${auraResult.reward}${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}`),
            field('XP', `+${xpResult.reward}`),
            field('Clan XP', `+${clanXpResult.gained || 0}`)
          )
          .setTimestamp()
      ],
      ephemeral: true
    });
  }

  if (interaction.customId === 'econ_bal') {
    updateRank(user);
    const embed = new EmbedBuilder()
      .setColor(EMBED_COLORS.success)
      .setTitle('Balance Overview')
      .addFields(
        field('Wallet', user.aura),
        field('Vault', user.vault),
        field('Rank', user.rank),
        field('Clan', user.clan ? `${user.clan} (Lv ${user.clanLevel})` : 'None')
      )
      .setTimestamp();

    return interaction.reply(visualReplyOptions(embed, getEconomyVisualKey(embed.data.title), { components: buildEconomyRows(user), ephemeral: true }));
  }

  if (interaction.customId === 'econ_deposit_all') {
    if (user.aura <= 0) {
      return interaction.reply(visualReplyOptions(
        createPlainInteractionEmbed('No Aura', EMBED_COLORS.danger, 'You have no Aura in your wallet to deposit.'),
        'core_profile',
        { ephemeral: true }
      ));
    }

    const amount = user.aura;
    user.aura = 0;
    user.vault += amount;
    user.lastVaultInterest = Date.now();
    updateRank(user);
    await user.save();

    return interaction.reply(visualReplyOptions(
      createPlainInteractionEmbed('Vault Deposit', EMBED_COLORS.success, `Deposited ${amount} Aura into your vault.`),
      'core_profile',
      {
      components: buildEconomyRows(user),
      ephemeral: true
      }
    ));
  }

  if (interaction.customId === 'econ_daily') {
    const cd = cooldown(user, 'daily', 86400000);
    if (cd) {
      return interaction.reply(visualReplyOptions(
        createPlainInteractionEmbed('Cooldown Active', EMBED_COLORS.danger, `Wait ${cd}s before claiming daily again.`),
        'core_profile',
        { ephemeral: true }
      ));
    }

    user.streak++;
    const auraResult = addAura(user, 5000 * user.streak, 'daily');
    const xpResult = addXp(user, 120 + user.streak * 10);
    const clanXpResult = await addClanXp(user.clan, 50 + user.streak * 5);
    xpResult.level = user.level;
    await user.save();

    return interaction.reply(visualReplyOptions(
      new EmbedBuilder()
        .setColor(EMBED_COLORS.success)
        .setTitle('Daily Reward')
        .setDescription(`Your streak is now **${user.streak}**.`)
        .addFields(
          field('Aura', `+${auraResult.reward}${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}`),
          field('XP', `+${xpResult.reward}`),
          field('Clan XP', `+${clanXpResult.gained || 0}`),
          field('Progress', `${levelProgress(user)}${user.clan ? `\n${clanLevelProgress(user)}` : ''}`, false)
        )
        .setTimestamp(),
      getEconomyVisualKey('Daily Reward'),
      {
      components: buildEconomyRows(user),
      ephemeral: true
      }
    ));
  }

  if (interaction.customId === 'econ_vaultinterest') {
    const nextIn = Math.max(VAULT_INTEREST_INTERVAL - (Date.now() - user.lastVaultInterest), 0);
    const effectiveRate = ((VAULT_INTEREST_RATE + getRankPerks(user).vaultBonus + getClanPerks(user).vaultBonus) * 100).toFixed(2);

    return interaction.reply(visualReplyOptions(
      new EmbedBuilder()
        .setColor(EMBED_COLORS.success)
        .setTitle('Vault Interest')
        .setDescription('Your vault grows passively over time.')
        .addFields(
          field('Vault', user.vault),
          field('Rate', `${effectiveRate}% every 24h`),
          field('Next Payout', formatDuration(nextIn))
        )
        .setTimestamp(),
      getEconomyVisualKey('Vault Interest'),
      {
      components: buildEconomyRows(user),
      ephemeral: true
      }
    ));
  }

  if (interaction.customId === 'econ_coinflip_modal') {
    const modal = new ModalBuilder()
      .setCustomId('modal_coinflip')
      .setTitle('Coinflip');

    const choiceInput = new TextInputBuilder()
      .setCustomId('choice')
      .setLabel('Choice: heads or tails')
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(5);

    const amountInput = new TextInputBuilder()
      .setCustomId('amount')
      .setLabel('Bet amount')
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(12);

    modal.addComponents(
      new ActionRowBuilder().addComponents(choiceInput),
      new ActionRowBuilder().addComponents(amountInput)
    );

    return interaction.showModal(modal);
  }

  if (interaction.customId === 'econ_deposit_modal') {
    const modal = new ModalBuilder()
      .setCustomId('modal_deposit')
      .setTitle('Deposit Aura');

    const amountInput = new TextInputBuilder()
      .setCustomId('amount')
      .setLabel('Amount or "all"')
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(12);

    modal.addComponents(new ActionRowBuilder().addComponents(amountInput));
    return interaction.showModal(modal);
  }

  if (interaction.customId === 'clan_create_modal') {
    const modal = new ModalBuilder()
      .setCustomId('modal_clan_create')
      .setTitle('Create Clan');

    const nameInput = new TextInputBuilder()
      .setCustomId('name')
      .setLabel('Clan name')
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(20);

    modal.addComponents(new ActionRowBuilder().addComponents(nameInput));
    return interaction.showModal(modal);
  }

  if (interaction.customId === 'clan_rename_modal') {
    const modal = new ModalBuilder()
      .setCustomId('modal_clan_rename')
      .setTitle('Rename Clan');

    const nameInput = new TextInputBuilder()
      .setCustomId('name')
      .setLabel('New clan name')
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(20);

    modal.addComponents(new ActionRowBuilder().addComponents(nameInput));
    return interaction.showModal(modal);
  }

  if (interaction.customId === 'skill_paths') {
    return interaction.reply(visualReplyOptions(
      buildHelpSectionEmbed({ author: interaction.user }, { name: 'Skill Paths', summary: 'Choose a specialization once a branch reaches level 5.', commands: [] }).setDescription(`Skill specializations unlock at branch level 5.\n${specializationSummary()}`),
      'help_skills',
      {
      components: [buildSkillPathRow('dmg'), buildSkillPathRow('defense'), buildSkillPathRow('luck')],
      ephemeral: true
      }
    ));
  }

  if (interaction.customId.startsWith('skill_upgrade:')) {
    const skillName = interaction.customId.split(':')[1];
    if (!['dmg', 'defense', 'luck'].includes(skillName)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Skill Missing', 'That skill branch was not found.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const points = availableSkillPoints(user);
    const cost = nextSkillCost(user, skillName);
    if (points < cost) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Not Enough Skill Points', `You need ${cost} skill points to upgrade ${skillName}. Available: ${points}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    user.skills[skillName] += 1;
    user.markModified('skills');
    await user.save();

    return interaction.reply(visualReplyOptions(
      interactionNoticeEmbed('Skill Upgraded', `Upgraded **${skillName}** to **${user.skills[skillName]}**.`, EMBED_COLORS.success),
      'help_skills',
      {
      components: [buildSkillUpgradeRow()],
      ephemeral: true
      }
    ));
  }

  if (interaction.customId.startsWith('skill_specialize:')) {
    const [, skillName, pathName] = interaction.customId.split(':');
    if (!['dmg', 'defense', 'luck'].includes(skillName) || !SKILL_SPECIALIZATIONS[skillName]?.[pathName]) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Path Missing', 'That specialization was not found.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (user.skills[skillName] < 5) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Branch Too Low', `You need ${skillName} level 5 to unlock a specialization.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    if (getSpecialization(user, skillName)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Already Specialized', `You already chose ${getSpecialization(user, skillName)} for ${skillName}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    user.specializations[skillName] = pathName;
    user.markModified('specializations');
    await user.save();

    return interaction.reply(visualReplyOptions(
      interactionNoticeEmbed('Specialization Chosen', `Specialized **${skillName}** into **${pathName}**.\n${SKILL_SPECIALIZATIONS[skillName][pathName]}`, EMBED_COLORS.success),
      'help_skills',
      {
      components: [buildSkillUpgradeRow()],
      ephemeral: true
      }
    ));
  }

  if (interaction.customId.startsWith('boss_start:')) {
    const bossKey = interaction.customId.split(':')[1];
    const boss = findBoss(bossKey);

    if (!boss) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Unknown Boss', 'Choose a valid boss.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (activeBossBattles.has(user.userId)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Boss Fight Active', 'You already have an active solo boss fight.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (user.level < boss.minLevel) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Level Too Low', `You need to be level ${boss.minLevel} to fight ${boss.name}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

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
    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle(`Solo Boss: ${boss.name}`)
          .setDescription(boss.description)
          .addFields(
            field('Boss HP', `${state.bossHp}/${boss.hp}`),
            field('Your HP', `${state.playerHp}/${state.maxPlayerHp}`),
            field('Heals Left', state.healsLeft),
            field('Rewards', `${boss.aura} Aura • ${boss.xp} XP • ${boss.clanXp} Clan XP`, false),
            field('Perk', boss.perk.label, false)
          )
          .setTimestamp()
      ],
      components: [buildSoloBossRow()],
      ephemeral: true
    });
  }

  if (interaction.customId === 'boss_attack' || interaction.customId === 'boss_heal' || interaction.customId === 'boss_status') {
    const state = activeBossBattles.get(user.userId);
    if (!state) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Boss Fight', 'You do not have an active solo boss fight.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (interaction.customId === 'boss_status') {
      return interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.danger)
            .setTitle(`Solo Boss: ${state.boss.name}`)
            .addFields(
              field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
              field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
              field('Heals Left', state.healsLeft),
              field('Reward Perk', state.boss.perk.label)
            )
            .setTimestamp()
        ],
        components: [buildSoloBossRow()],
        ephemeral: true
      });
    }

    if (interaction.customId === 'boss_attack') {
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

        return interaction.reply({
          embeds: [
            new EmbedBuilder()
              .setColor(EMBED_COLORS.success)
              .setTitle(`Boss Defeated: ${state.boss.name}`)
              .setDescription(`Final hit: ${hit.damage}${hit.crit ? ' (CRIT)' : ''}`)
              .addFields(
                field('Aura', `+${auraResult.reward}`),
                field('XP', `+${xpResult.reward}`),
                field('Clan XP', `+${clanXpResult.gained || 0}`),
                field('Perk', perkGranted ? state.boss.perk.label : `${state.boss.perk.label} already active`, false)
              )
              .setTimestamp()
          ],
          components: [buildBossStartRow()],
          ephemeral: true
        });
      }

      const counter = bossCounterDamage(user, state.boss);
      state.playerHp -= counter.damage;

      if (state.playerHp <= 0) {
        activeBossBattles.delete(user.userId);
        return interaction.reply({
          embeds: [
            interactionNoticeEmbed('Defeat', `The boss ended the fight ${counter.dodged ? 'after a dodged counter' : `with ${counter.damage} damage`}.`, EMBED_COLORS.danger)
          ],
          components: [buildBossStartRow()],
          ephemeral: true
        });
      }

      return interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.danger)
            .setTitle(`Boss Turn: ${state.boss.name}`)
            .setDescription(`You dealt ${hit.damage}${hit.crit ? ' (CRIT)' : ''}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
            .addFields(
              field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
              field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
              field('Heals Left', state.healsLeft)
            )
            .setTimestamp()
        ],
        components: [buildSoloBossRow()],
        ephemeral: true
      });
    }

    if (state.healsLeft <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Heals Left', 'You have no heals left in this boss fight.', EMBED_COLORS.danger)], ephemeral: true });
    }
    if (state.playerHp >= state.maxPlayerHp) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('HP Full', 'Your HP is already full.', EMBED_COLORS.info)], ephemeral: true });
    }

    const healed = bossHealAmount(user, state.maxPlayerHp - state.playerHp);
    state.playerHp += healed;
    state.healsLeft -= 1;

    const counter = bossCounterDamage(user, state.boss);
    state.playerHp -= counter.damage;

    if (state.playerHp <= 0) {
      activeBossBattles.delete(user.userId);
      return interaction.reply({
        embeds: [interactionNoticeEmbed('Defeat', `You healed for ${healed}, but the boss still won.`, EMBED_COLORS.danger)],
        components: [buildBossStartRow()],
        ephemeral: true
      });
    }

    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle(`Boss Heal: ${state.boss.name}`)
          .setDescription(`You healed for ${healed}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
          .addFields(
            field('Boss HP', `${Math.max(state.bossHp, 0)}/${state.boss.hp} ${bar(Math.max(state.bossHp, 0), state.boss.hp)}`, false),
            field('Your HP', `${Math.max(state.playerHp, 0)}/${state.maxPlayerHp} ${bar(Math.max(state.playerHp, 0), state.maxPlayerHp)}`, false),
            field('Heals Left', state.healsLeft)
          )
          .setTimestamp()
      ],
      components: [buildSoloBossRow()],
      ephemeral: true
    });
  }

  if (interaction.customId === 'boss_clan_join' || interaction.customId === 'boss_clan_attack' || interaction.customId === 'boss_clan_heal' || interaction.customId === 'boss_clan_status') {
    if (!user.clan) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Clan', 'You need to be in a clan to use clan boss raids.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const raid = getActiveClanBossRaid(user.clan);
    if (!raid) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Active Raid', 'Your clan does not have an active boss raid.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (interaction.customId === 'boss_clan_join') {
      const cap = clanBossParticipantCap(raid.clanLevel || 1);
      if (raid.participants.includes(user.userId)) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('Already Joined', 'You are already in the clan boss raid.', EMBED_COLORS.danger)], ephemeral: true });
      }
      if (raid.participants.length >= cap) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('Raid Full', `The clan boss raid is full. Cap: ${cap}.`, EMBED_COLORS.danger)], ephemeral: true });
      }

      raid.participants.push(user.userId);
      raid.playerMaxHp[user.userId] = battleMaxHp(user);
      raid.playerHp[user.userId] = raid.playerMaxHp[user.userId];
      raid.healsLeft[user.userId] = 3;
      raid.damageByUser[user.userId] = raid.damageByUser[user.userId] || 0;
      raid.logs.push(`${interaction.user.username} joined the raid.`);

      return interaction.reply({
        embeds: [interactionNoticeEmbed('Raid Joined', `You joined the clan boss raid for **${user.clan}**.`, EMBED_COLORS.success)],
        components: [buildClanBossRow()],
        ephemeral: true
      });
    }

    if (interaction.customId === 'boss_clan_status') {
      const roster = raid.participants.length > 0
        ? raid.participants.map(id => `<@${id}> • ${Math.max(raid.playerHp[id] || 0, 0)}/${raid.playerMaxHp[id] || 1}`).join('\n')
        : 'No participants yet.';

      return interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.danger)
            .setTitle(`Clan Raid: ${raid.boss.name}`)
            .setDescription(`Clan: **${raid.clanName}**`)
            .addFields(
              field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
              field('Participants', raid.participants.length),
              field('Top Logs', raid.logs.slice(-4).join('\n') || 'No logs yet.', false),
              field('Roster', roster, false)
            )
            .setTimestamp()
        ],
        components: [buildClanBossRow()],
        ephemeral: true
      });
    }

    if (!raid.participants.includes(user.userId)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Join The Raid', 'Join the raid first.', EMBED_COLORS.danger)], ephemeral: true });
    }
    if ((raid.playerHp[user.userId] || 0) <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Raid KO', `You are down in this raid and cannot ${interaction.customId === 'boss_clan_attack' ? 'attack' : 'heal'}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    if (interaction.customId === 'boss_clan_attack') {
      const hit = bossPlayerAttack(user, raid.boss);
      raid.bossHp -= hit.damage;
      raid.damageByUser[user.userId] = (raid.damageByUser[user.userId] || 0) + hit.damage;
      raid.logs.push(`${interaction.user.username} dealt ${hit.damage}${hit.crit ? ' crit' : ''} damage.`);

      if (raid.bossHp <= 0) {
        const rewards = await rewardClanBossRaid(raid);
        activeClanBossRaids.delete(raid.clanName);
        return interaction.reply({
          embeds: [
            new EmbedBuilder()
              .setColor(EMBED_COLORS.success)
              .setTitle(`Clan Boss Defeated: ${raid.boss.name}`)
              .setDescription(`**${raid.clanName}** cleared the raid.`)
              .addFields(
                field('Top Damage', rewards.topDamagerId ? `<@${rewards.topDamagerId}>` : 'None'),
                field('Perk Awarded', raid.boss.perk.label),
                field('Clan XP', `+${rewards.clanXpResult.gained || 0}`),
                field('Rewards', rewards.rewardLines.join('\n'), false)
              )
              .setTimestamp()
          ],
          components: [buildBossStartRow()],
          ephemeral: true
        });
      }

      const counter = bossCounterDamage(user, raid.boss);
      raid.playerHp[user.userId] -= counter.damage;
      raid.logs.push(counter.dodged ? `${interaction.user.username} dodged the counter.` : `${interaction.user.username} took ${counter.damage} counter damage.`);

      const livingMembers = raid.participants.filter(id => (raid.playerHp[id] || 0) > 0);
      if (livingMembers.length === 0) {
        activeClanBossRaids.delete(raid.clanName);
        return interaction.reply({
          embeds: [interactionNoticeEmbed('Raid Failed', `The full **${raid.clanName}** roster was defeated.`, EMBED_COLORS.danger)],
          components: [buildBossStartRow()],
          ephemeral: true
        });
      }

      return interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.danger)
            .setTitle(`Raid Turn: ${raid.boss.name}`)
            .setDescription(`You dealt ${hit.damage}${hit.crit ? ' (CRIT)' : ''}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
            .addFields(
              field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
              field('Your HP', `${Math.max(raid.playerHp[user.userId], 0)}/${raid.playerMaxHp[user.userId]}`),
              field('Roster Alive', livingMembers.length)
            )
            .setTimestamp()
        ],
        components: [buildClanBossRow()],
        ephemeral: true
      });
    }

    if ((raid.healsLeft[user.userId] || 0) <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Heals Left', 'You have no heals left in this raid.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const maxHp = raid.playerMaxHp[user.userId];
    if (raid.playerHp[user.userId] >= maxHp) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('HP Full', 'Your HP is already full.', EMBED_COLORS.info)], ephemeral: true });
    }

    const healed = bossHealAmount(user, maxHp - raid.playerHp[user.userId]);
    raid.playerHp[user.userId] += healed;
    raid.healsLeft[user.userId] -= 1;
    const counter = bossCounterDamage(user, raid.boss);
    raid.playerHp[user.userId] -= counter.damage;
    raid.logs.push(`${interaction.user.username} healed for ${healed}.`);

    const livingMembers = raid.participants.filter(id => (raid.playerHp[id] || 0) > 0);
    if (livingMembers.length === 0) {
      activeClanBossRaids.delete(raid.clanName);
      return interaction.reply({
        embeds: [interactionNoticeEmbed('Raid Failed', `You healed for ${healed}, but the raid still collapsed.`, EMBED_COLORS.danger)],
        components: [buildBossStartRow()],
        ephemeral: true
      });
    }

    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle(`Raid Heal: ${raid.boss.name}`)
          .setDescription(`You healed for ${healed}. Boss counter: ${counter.dodged ? 'dodged' : `${counter.damage} damage`}.`)
          .addFields(
            field('Boss HP', `${Math.max(raid.bossHp, 0)}/${raid.boss.clanHp} ${bar(Math.max(raid.bossHp, 0), raid.boss.clanHp)}`, false),
            field('Your HP', `${Math.max(raid.playerHp[user.userId], 0)}/${raid.playerMaxHp[user.userId]}`),
            field('Heals Left', raid.healsLeft[user.userId])
          )
          .setTimestamp()
      ],
      components: [buildClanBossRow()],
      ephemeral: true
    });
  }

  if (interaction.customId === 'clanwar_join' || interaction.customId === 'clanwar_leave' || interaction.customId === 'clanwar_status' || interaction.customId === 'clanwar_start') {
    if (!user.clan) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Clan', 'You are not in a clan.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const war = getActiveClanWar(user.clan);
    if (!war) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('No Active War', 'Your clan does not have an active clan war.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (interaction.customId === 'clanwar_status') {
      const attackerCap = clanWarParticipantCap(war.attackerLevel);
      const defenderCap = clanWarParticipantCap(war.defenderLevel);
      const recentLogs = war.logs.slice(-5).join('\n') || 'No logs yet.';

      return interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setColor(EMBED_COLORS.danger)
            .setTitle('Clan War Status')
            .setDescription(`**${war.attackerClan}** vs **${war.defenderClan}**`)
            .addFields(
              field('Started', war.started ? 'Yes' : 'No'),
              field(`${war.attackerClan} Roster`, `${war.attackerParticipants.length}/${attackerCap}`),
              field(`${war.defenderClan} Roster`, `${war.defenderParticipants.length}/${defenderCap}`),
              field('Recent Logs', recentLogs, false)
            )
            .setTimestamp()
        ],
        components: [buildClanWarRow(user, war)],
        ephemeral: true
      });
    }

    const rosterKey = war.attackerClan === user.clan ? 'attackerParticipants' : 'defenderParticipants';
    const clanLevel = war.attackerClan === user.clan ? war.attackerLevel : war.defenderLevel;
    const cap = clanWarParticipantCap(clanLevel);

    if (interaction.customId === 'clanwar_join') {
      if (war.started) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('War Started', 'This clan war has already started.', EMBED_COLORS.danger)], ephemeral: true });
      }
      if (war[rosterKey].includes(user.userId)) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('Already Joined', 'You are already on the war roster.', EMBED_COLORS.danger)], ephemeral: true });
      }
      if (war[rosterKey].length >= cap) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('Roster Full', `Your clan war roster is full. Cap: ${cap}.`, EMBED_COLORS.danger)], ephemeral: true });
      }

      war[rosterKey].push(user.userId);
      war.logs.push(`${interaction.user.username} joined the ${user.clan} war roster.`);
      return interaction.reply({
        embeds: [interactionNoticeEmbed('Clan War Roster', `You joined the war roster for **${user.clan}**.`, EMBED_COLORS.success)],
        components: [buildClanWarRow(user, war)],
        ephemeral: true
      });
    }

    if (interaction.customId === 'clanwar_leave') {
      if (war.started) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('War Started', 'This clan war has already started.', EMBED_COLORS.danger)], ephemeral: true });
      }

      war[rosterKey] = war[rosterKey].filter(id => id !== user.userId);
      war.logs.push(`${interaction.user.username} left the ${user.clan} war roster.`);
      return interaction.reply({
        embeds: [interactionNoticeEmbed('Clan War Roster', `You left the war roster for **${user.clan}**.`, EMBED_COLORS.danger)],
        components: [buildClanWarRow(user, war)],
        ephemeral: true
      });
    }

    if (user.clanRole !== 'owner' || (user.userId !== war.attackerOwnerId && user.userId !== war.defenderOwnerId)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Owner Only', 'Only the two clan owners can start this war.', EMBED_COLORS.danger)], ephemeral: true });
    }
    if (war.started) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('War Started', 'This clan war has already started.', EMBED_COLORS.danger)], ephemeral: true });
    }
    if (war.attackerParticipants.length === 0 || war.defenderParticipants.length === 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Need Rosters', 'Both clans need at least one participant before the war can start.', EMBED_COLORS.danger)], ephemeral: true });
    }

    war.started = true;
    const result = await resolveClanWar(war);
    activeClanWars.delete(war.key);

    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle('Clan War Result')
          .setDescription(`Winner: **${result.winnerClan}**`)
          .addFields(
            field('Loser', result.loserClan),
            field('Score', `${war.attackerClan} ${result.attackerRounds} - ${war.defenderClan} ${result.defenderRounds}`),
            field('Rewards', `Winner members +6000 Aura • Loser members +2000 Aura • +${result.winnerClanXp.gained || 0} Clan XP`, false),
            field('War Log', war.logs.slice(-6).join('\n'), false)
          )
          .setTimestamp()
      ],
      components: [],
      ephemeral: true
    });
  }

  if (interaction.customId.startsWith('claninvite_accept:')) {
    const [, clanToken, targetId] = interaction.customId.split(':');
    const clanName = decodeClanToken(clanToken);

    if (interaction.user.id !== targetId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Invite Locked', 'Only the invited player can accept this clan invite.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (user.clan) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Already In Clan', `You are already in **${user.clan}**.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    const inviteName = user.clanInvites.find(invite => invite.toLowerCase() === clanName.toLowerCase());
    if (!inviteName) {
      pendingClanInvites.delete(`${clanName}:${targetId}`);
      return interaction.reply({ embeds: [interactionNoticeEmbed('Invite Missing', 'This invite is no longer available.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const existingClan = await User.findOne({ clan: inviteName });
    if (!existingClan) {
      user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
      await user.save();
      pendingClanInvites.delete(`${clanName}:${targetId}`);
      return interaction.update({
        embeds: [interactionNoticeEmbed('Clan Missing', 'That clan no longer exists.', EMBED_COLORS.danger)],
        components: []
      });
    }

    const clanPerks = getClanLevelData(existingClan.clanLevel || 1).perks;
    const memberCount = await User.countDocuments({ clan: existingClan.clan });
    if (memberCount >= clanPerks.memberCap) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Full', `**${existingClan.clan}** is full. Member cap: ${clanPerks.memberCap}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    user.clan = existingClan.clan;
    user.clanRole = 'member';
    user.clanPrivacy = existingClan.clanPrivacy || 'private';
    user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
    user.clanLevel = existingClan.clanLevel || 1;
    user.clanXp = existingClan.clanXp || 0;
    await user.save();
    pendingClanInvites.delete(`${existingClan.clan}:${targetId}`);

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle('Clan Invite Accepted')
          .setDescription(`<@${interaction.user.id}> joined **${existingClan.clan}**.`)
          .addFields(
            field('Clan Level', user.clanLevel),
            field('Privacy', user.clanPrivacy)
          )
          .setTimestamp()
      ],
      components: []
    });
  }

  if (interaction.customId.startsWith('claninvite_decline:')) {
    const [, clanToken, targetId] = interaction.customId.split(':');
    const clanName = decodeClanToken(clanToken);

    if (interaction.user.id !== targetId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Invite Locked', 'Only the invited player can decline this clan invite.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const inviteName = user.clanInvites.find(invite => invite.toLowerCase() === clanName.toLowerCase());
    if (!inviteName) {
      pendingClanInvites.delete(`${clanName}:${targetId}`);
      return interaction.reply({ embeds: [interactionNoticeEmbed('Invite Missing', 'This invite is no longer available.', EMBED_COLORS.danger)], ephemeral: true });
    }

    user.clanInvites = user.clanInvites.filter(invite => invite !== inviteName);
    await user.save();
    pendingClanInvites.delete(`${inviteName}:${targetId}`);

    return interaction.update({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle('Clan Invite Declined')
          .setDescription(`<@${interaction.user.id}> declined the invite to **${inviteName}**.`)
          .setTimestamp()
      ],
      components: []
    });
  }

  if (interaction.customId.startsWith('clanjoin_public:')) {
    const [, clanToken] = interaction.customId.split(':');
    const clanName = decodeClanToken(clanToken);

    if (user.clan) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Already In Clan', `You are already in **${user.clan}**.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    const existingClan = await User.findOne({ clan: clanName });
    if (!existingClan) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Missing', 'That clan no longer exists.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if ((existingClan.clanPrivacy || 'private') !== 'public') {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Private Clan', 'This clan is private and requires an invite.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const clanPerks = getClanLevelData(existingClan.clanLevel || 1).perks;
    const memberCount = await User.countDocuments({ clan: existingClan.clan });
    if (memberCount >= clanPerks.memberCap) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Full', `**${existingClan.clan}** is full. Member cap: ${clanPerks.memberCap}.`, EMBED_COLORS.danger)], ephemeral: true });
    }

    user.clan = existingClan.clan;
    user.clanRole = 'member';
    user.clanPrivacy = existingClan.clanPrivacy || 'private';
    user.clanInvites = user.clanInvites.filter(invite => invite !== existingClan.clan);
    user.clanLevel = existingClan.clanLevel || 1;
    user.clanXp = existingClan.clanXp || 0;
    await user.save();
    pendingClanInvites.delete(`${existingClan.clan}:${interaction.user.id}`);

    return interaction.reply({
      embeds: [
        interactionNoticeEmbed('Clan Joined', `You joined **${existingClan.clan}**.`, EMBED_COLORS.success)
      ],
      ephemeral: true
    });
  }

  if (interaction.customId.startsWith('clanprivacy_public:') || interaction.customId.startsWith('clanprivacy_private:')) {
    const [action, clanToken, ownerId] = interaction.customId.split(':');
    const clanName = decodeClanToken(clanToken);
    const mode = action.endsWith('_public') ? 'public' : 'private';

    if (interaction.user.id !== ownerId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Owner Only', 'Only the clan owner can change clan privacy from this button.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (user.clan !== clanName || user.clanRole !== 'owner') {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Changed', 'You are no longer the owner of this clan.', EMBED_COLORS.danger)], ephemeral: true });
    }

    await syncClanSettings(clanName, { clanPrivacy: mode });
    user.clanPrivacy = mode;
    await user.save();

    return interaction.reply({
      embeds: [
        interactionNoticeEmbed('Clan Privacy Updated', `**${clanName}** is now **${mode}**.`, EMBED_COLORS.success)
      ],
      ephemeral: true
    });
  }

  if (interaction.customId.startsWith('clanleave:')) {
    const [, clanToken] = interaction.customId.split(':');
    const clanName = decodeClanToken(clanToken);

    if (user.clan !== clanName) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Changed', 'You are not in that clan anymore.', EMBED_COLORS.danger)], ephemeral: true });
    }

    if (user.clanRole === 'owner') {
      const otherMembers = await User.countDocuments({ clan: user.clan, userId: { $ne: user.userId } });
      if (otherMembers > 0) {
        return interaction.reply({ embeds: [interactionNoticeEmbed('Transfer First', 'Transfer ownership before leaving your clan.', EMBED_COLORS.danger)], ephemeral: true });
      }
    }

    const oldClan = user.clan;
    user.clan = null;
    user.clanRole = null;
    user.clanPrivacy = 'private';
    user.clanLevel = 1;
    user.clanXp = 0;
    await user.save();

    return interaction.reply({
      embeds: [
        interactionNoticeEmbed('Clan Left', `You left **${oldClan}**.`, EMBED_COLORS.success)
      ],
      ephemeral: true
    });
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
      ...visualReplyOptions(
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle('PvP Battle Started')
          .setDescription(`First turn: <@${firstTurn}>`)
          .addFields(
            field('Challenger HP', `${battle.hp[challengerId]}/${battleMaxHp(challenger)} ${bar(battle.hp[challengerId], battleMaxHp(challenger))}`, false),
            field('Opponent HP', `${battle.hp[targetId]}/${battleMaxHp(challenged)} ${bar(battle.hp[targetId], battleMaxHp(challenged))}`, false),
            field('Battle Rules', 'Each player has 2 heals and 1 critical strike.', false)
          )
          .setTimestamp(),
        getPvpVisualKey('PvP Battle Started'),
        { components: [buildBattleRow(false)] }
      )
    });
  }

  if (interaction.customId.startsWith('pvp_decline:')) {
    const [, challengerId, targetId] = interaction.customId.split(':');
    if (interaction.user.id !== targetId) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Challenge Locked', 'Only the challenged player can decline this fight.', EMBED_COLORS.danger)], ephemeral: true });
    }

    pendingChallenges.delete(`${challengerId}:${targetId}`);
    return interaction.update({
      ...visualReplyOptions(
        combatEmbed(
          'PvP Challenge Declined',
          EMBED_COLORS.danger,
          `<@${targetId}> declined the PvP challenge from <@${challengerId}>.`
        ),
        getPvpVisualKey('PvP Challenge Declined'),
        { components: [] }
      )
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
      attacker.wins++;
      defender.losses++;
      const auraResult = addAura(attacker, 10000, 'aura');
      const xpResult = addXp(attacker, 150);
      const clanXpResult = await addClanXp(attacker.clan, 75);
      xpResult.level = attacker.level;

      await attacker.save();
      await defender.save();
      battles.delete(interaction.user.id);
      battles.delete(enemyId);

      return interaction.update({
        ...visualReplyOptions(
          new EmbedBuilder()
            .setColor(EMBED_COLORS.success)
            .setTitle('PvP Victory')
            .setDescription(`+${auraResult.reward} Aura${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}${formatProgressExtras(xpResult)}${formatClanProgressExtras(clanXpResult)}`)
            .setTimestamp(),
          getPvpVisualKey('PvP Victory'),
          { components: [buildBattleRow(true)] }
        )
      });
    }

    return interaction.update({
      ...visualReplyOptions(
        new EmbedBuilder()
          .setColor(EMBED_COLORS.danger)
          .setTitle('PvP Turn')
          .setDescription(result.dodged ? 'Enemy dodged your attack.' : `Damage dealt: ${result.damage}${result.crit ? ' (CRIT)' : ''}`)
          .addFields(
            field('Next Turn', `<@${enemyId}>`),
            field('Your HP', `${battle.hp[interaction.user.id]}/${battleMaxHp(attacker)} ${bar(battle.hp[interaction.user.id], battleMaxHp(attacker))}`, false),
            field('Enemy HP', `${Math.max(battle.hp[enemyId], 0)}/${battleMaxHp(defender)} ${bar(Math.max(battle.hp[enemyId], 0), battleMaxHp(defender))}`, false)
          )
          .setTimestamp(),
        getPvpVisualKey('PvP Turn'),
        { components: [buildBattleRow(false)] }
      )
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
      ...visualReplyOptions(
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle('PvP Heal')
          .setDescription(`You restored ${finalHeal} HP.`)
          .addFields(
            field('Heals Left', battle.healsLeft[interaction.user.id]),
            field('Next Turn', `<@${enemyId}>`),
            field('Your HP', `${battle.hp[interaction.user.id]}/${maxHp} ${bar(battle.hp[interaction.user.id], maxHp)}`, false)
          )
          .setTimestamp(),
        getPvpVisualKey('PvP Heal'),
        { components: [buildBattleRow(false)] }
      )
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
      ...visualReplyOptions(
        new EmbedBuilder()
          .setColor(EMBED_COLORS.info)
          .setTitle('PvP Defend')
          .setDescription('Defend activated. Your next incoming hit will be reduced.')
          .addFields(field('Next Turn', `<@${enemyId}>`))
          .setTimestamp(),
        getPvpVisualKey('PvP Defend'),
        { components: [buildBattleRow(false)] }
      )
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
      attacker.wins++;
      defender.losses++;
      const auraResult = addAura(attacker, 10000, 'aura');
      const xpResult = addXp(attacker, 150);
      const clanXpResult = await addClanXp(attacker.clan, 75);
      xpResult.level = attacker.level;

      await attacker.save();
      await defender.save();
      battles.delete(interaction.user.id);
      battles.delete(enemyId);

      return interaction.update({
        ...visualReplyOptions(
          new EmbedBuilder()
            .setColor(EMBED_COLORS.success)
            .setTitle('Critical Victory')
            .setDescription(`${result.dodged ? 'Enemy dodged the strike.' : `${result.damage}${result.crit ? ' damage (CRIT)' : ' damage'}`}\n+${auraResult.reward} Aura${auraResult.multiplier > 1 ? ` (${auraResult.multiplier}x boost)` : ''}${formatProgressExtras(xpResult)}${formatClanProgressExtras(clanXpResult)}`)
            .setTimestamp(),
          getPvpVisualKey('Critical Victory'),
          { components: [buildBattleRow(true)] }
        )
      });
    }

    return interaction.update({
      ...visualReplyOptions(
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
          .setTimestamp(),
        getPvpVisualKey('Critical Strike'),
        { components: [buildBattleRow(false)] }
      )
    });
  }
});

client.on('interactionCreate', async (interaction) => {
  wrapReplyMethod(interaction, 'reply');
  wrapReplyMethod(interaction, 'followUp');
  wrapReplyMethod(interaction, 'editReply');
  if (!interaction.isModalSubmit()) return;
  if (!interaction.guild) {
    return interaction.reply({ embeds: [interactionNoticeEmbed('Server Only', 'These forms can only be used inside a server.', EMBED_COLORS.danger)], ephemeral: true });
  }

  const { config: guildConfig } = await ensureValidBotChannel(
    interaction.guild,
    await getGuildConfig(interaction.guild.id)
  );
  if (guildConfig.botChannelId && interaction.channelId !== guildConfig.botChannelId) {
    return interaction.reply({ embeds: [interactionNoticeEmbed('Wrong Channel', `Use bot interactions in <#${guildConfig.botChannelId}>.`, EMBED_COLORS.danger)], ephemeral: true });
  }

  const user = await getUser(interaction.user.id);
  removeExpiredBoosts(user);
  const vaultInterest = applyVaultInterest(user);
  if (vaultInterest.applied > 0) {
    await user.save();
  }

  if (interaction.customId === 'modal_coinflip') {
    const choice = interaction.fields.getTextInputValue('choice').trim().toLowerCase();
    let bet = parseInt(interaction.fields.getTextInputValue('amount').trim(), 10);

    if (!['heads', 'tails'].includes(choice)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Invalid Choice', 'Choose `heads` or `tails`.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const cd = cooldown(user, 'cf', 120000);
    if (cd) {
      return interaction.reply(visualReplyOptions(
        createPlainInteractionEmbed('Cooldown Active', EMBED_COLORS.danger, `Wait ${cd}s before using coinflip again.`),
        getCoreVisualKey('Coinflip'),
        { ephemeral: true }
      ));
    }

    if (!bet || bet <= 0) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Invalid Bet', 'Enter a valid amount to bet.', EMBED_COLORS.danger)], ephemeral: true });
    }
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
    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(won ? EMBED_COLORS.success : EMBED_COLORS.danger)
          .setTitle('Coinflip')
          .setDescription(`You chose **${choice}**. The coin landed on **${outcome}**.`)
          .addFields(
            field('Result', won ? 'Win' : 'Loss'),
            field('Aura', won ? `+${auraWon}` : 'No reward'),
            field('XP / Clan XP', won ? `+${xpWon} XP • +${clanXpWon} Clan XP` : 'No reward', false)
          )
          .setTimestamp()
      ],
      components: buildEconomyRows(user),
      ephemeral: true
    });
  }

  if (interaction.customId === 'modal_deposit') {
    const amountArg = interaction.fields.getTextInputValue('amount').trim().toLowerCase();
    const amount = amountArg === 'all' ? user.aura : parseInt(amountArg, 10);

    if (!Number.isInteger(amount) || amount <= 0) {
      return interaction.reply(visualReplyOptions(
        createPlainInteractionEmbed('Invalid Amount', EMBED_COLORS.danger, 'Enter a valid deposit amount.'),
        'core_profile',
        { ephemeral: true }
      ));
    }
    if (amount > user.aura) {
      return interaction.reply(visualReplyOptions(
        createPlainInteractionEmbed('Not Enough Aura', EMBED_COLORS.danger, "You don't have that much Aura in your wallet."),
        'core_profile',
        { ephemeral: true }
      ));
    }

    user.aura -= amount;
    user.vault += amount;
    user.lastVaultInterest = Date.now();
    updateRank(user);
    await user.save();

    return interaction.reply(visualReplyOptions(
      createPlainInteractionEmbed('Vault Deposit', EMBED_COLORS.success, `Deposited ${amount} Aura into your vault.`),
      'core_profile',
      {
      components: buildEconomyRows(user),
      ephemeral: true
      }
    ));
  }

  if (interaction.customId === 'modal_clan_create') {
    const clanName = normalizeClanName(interaction.fields.getTextInputValue('name'));
    if (!clanName) return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Name Missing', 'Enter a clan name.', EMBED_COLORS.danger)], ephemeral: true });
    if (!isValidClanName(clanName)) return interaction.reply({ embeds: [interactionNoticeEmbed('Invalid Clan Name', 'Clan names must be 3-20 characters and use only letters, numbers, and spaces.', EMBED_COLORS.danger)], ephemeral: true });
    if (user.clan) return interaction.reply({ embeds: [interactionNoticeEmbed('Already In Clan', `You are already in **${user.clan}**.`, EMBED_COLORS.danger)], ephemeral: true });
    if (user.aura < CLAN_CREATE_COST) return interaction.reply({ embeds: [interactionNoticeEmbed('Not Enough Aura', `You need ${CLAN_CREATE_COST} Aura to create a clan.`, EMBED_COLORS.danger)], ephemeral: true });

    const existingClan = await User.findOne({ clan: new RegExp(`^${clanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') });
    if (existingClan) return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Name Taken', 'That clan name is already taken.', EMBED_COLORS.danger)], ephemeral: true });

    user.aura -= CLAN_CREATE_COST;
    user.clan = clanName;
    user.clanRole = 'owner';
    user.clanPrivacy = 'private';
    user.clanInvites = [];
    user.clanLevel = 1;
    user.clanXp = 0;
    updateRank(user);
    await user.save();

    return interaction.reply({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.success)
          .setTitle(`Clan Created: ${clanName}`)
          .setDescription(`Your clan is live and ${CLAN_CREATE_COST} Aura has been spent.`)
          .addFields(
            field('Owner', `<@${user.userId}>`),
            field('Clan Level', user.clanLevel),
            field('Privacy', user.clanPrivacy),
            field('Member Cap', getClanLevelData(user.clanLevel).perks.memberCap),
            field('Wallet Left', user.aura)
          )
          .setTimestamp()
      ],
      components: [buildClanRenameRow()],
      ephemeral: true
    });
  }

  if (interaction.customId === 'modal_clan_rename') {
    if (!user.clan) return interaction.reply({ embeds: [interactionNoticeEmbed('No Clan', 'You are not in a clan.', EMBED_COLORS.danger)], ephemeral: true });
    if (user.clanRole !== 'owner') return interaction.reply({ embeds: [interactionNoticeEmbed('Owner Only', 'Only the clan owner can rename the clan.', EMBED_COLORS.danger)], ephemeral: true });

    const newClanName = normalizeClanName(interaction.fields.getTextInputValue('name'));
    if (!newClanName) return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Name Missing', 'Enter a new clan name.', EMBED_COLORS.danger)], ephemeral: true });
    if (!isValidClanName(newClanName)) return interaction.reply({ embeds: [interactionNoticeEmbed('Invalid Clan Name', 'Clan names must be 3-20 characters and use only letters, numbers, and spaces.', EMBED_COLORS.danger)], ephemeral: true });
    if (newClanName.toLowerCase() === user.clan.toLowerCase()) return interaction.reply({ embeds: [interactionNoticeEmbed('Same Name', 'That is already your clan name.', EMBED_COLORS.danger)], ephemeral: true });
    if (getActiveClanWar(user.clan) || hasPendingClanWar(user.clan) || getActiveClanBossRaid(user.clan)) {
      return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Busy', 'Finish active clan wars, pending clan wars, and clan raids before renaming the clan.', EMBED_COLORS.danger)], ephemeral: true });
    }

    const oldClanName = user.clan;
    const existingClan = await User.findOne({ clan: new RegExp(`^${newClanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i') });
    if (existingClan) return interaction.reply({ embeds: [interactionNoticeEmbed('Clan Name Taken', 'That clan name is already taken.', EMBED_COLORS.danger)], ephemeral: true });

    await User.updateMany({ clan: oldClanName }, { $set: { clan: newClanName } });

    const invitedUsers = await User.find({ clanInvites: oldClanName });
    for (const invitedUser of invitedUsers) {
      invitedUser.clanInvites = invitedUser.clanInvites.map(invite => invite === oldClanName ? newClanName : invite);
      await invitedUser.save();
    }

    for (const [key, invite] of pendingClanInvites.entries()) {
      if (invite.clanName === oldClanName) {
        pendingClanInvites.delete(key);
        pendingClanInvites.set(`${newClanName}:${invite.targetId}`, { ...invite, clanName: newClanName });
      }
    }

    user.clan = newClanName;
    await user.save();

    return interaction.reply({
      embeds: [
        interactionNoticeEmbed('Clan Renamed', `Your clan is now **${newClanName}**.`, EMBED_COLORS.success)
      ],
      components: [buildClanRenameRow()],
      ephemeral: true
    });
  }
});

// ================= EXPRESS WEB =================
const app = express();
app.get('/', (req, res) => res.send('Bot running'));
app.get('/healthz', (req, res) => res.status(200).send('ok'));
app.use('/generated', express.static(GENERATED_VISUAL_DIR, {
  immutable: true,
  maxAge: '365d'
}));
app.get('/visuals/local/:key/:kind.png', (req, res) => {
  const key = String(req.params.key || '');
  const kind = req.params.kind === 'thumb' ? 'thumb' : 'banner';
  const filename = LOCAL_VISUALS[key];

  if (!filename) {
    return res.status(404).send('visual not found');
  }

  try {
    const filePath = path.join(VISUAL_ASSET_DIR, filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).send('visual source missing');
    }

    const thumbnailFilename = LOCAL_VISUAL_THUMBNAILS[key] || filename;
    const thumbnailPath = path.join(VISUAL_ASSET_DIR, thumbnailFilename);
    const sourcePath = kind === 'thumb' && fs.existsSync(thumbnailPath) ? thumbnailPath : filePath;
    const width = kind === 'thumb' ? 256 : 1200;
    const cacheKey = `${kind === 'thumb' ? 'local-thumb' : 'local-banner'}:${key}:${width}`;
    const { buffer, version } = getCachedRenderedPng(cacheKey, sourcePath, width);

    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    res.setHeader('ETag', version);
    return res.send(Buffer.from(buffer));
  } catch (error) {
    console.error(`[visuals] failed to serve public visual key="${key}" kind="${kind}"`, error);
    return res.status(500).send('visual render failed');
  }
});
app.get('/api/leaderboard', async (req, res) => {
  const top = await User.find().sort({ aura: -1 }).limit(10);
  res.json(top);
});
app.listen(PORT, () => console.log(`Web API running on port ${PORT}`));

// ================= READY =================
client.on('guildCreate', async (guild) => {
  try {
    await getGuildConfig(guild.id);

    const targetChannel =
      guild.systemChannel ||
      guild.channels.cache.find(channel => channel.isTextBased?.() && channel.permissionsFor(guild.members.me)?.has(PermissionsBitField.Flags.SendMessages));

    if (!targetChannel) return;

    await targetChannel.send({
      embeds: [
        new EmbedBuilder()
          .setColor(EMBED_COLORS.info)
          .setTitle('Setup Required')
          .setDescription('Run `!setup #channel` to choose the only channel where I will respond. If you run `!setup` without tagging a channel, I will use the current one.')
          .setTimestamp()
      ]
    });
  } catch (error) {
    console.error('Failed to send guild setup message:', error);
  }
});

client.once('clientReady', () => {
  discordReady = true;
  clearTimeout(discordLoginWatchdog);
  console.log(`Logged in as ${client.user.tag}`);
  runVisualSelfTest();
  runPublicVisualSelfTest();
});

client.on('error', (error) => {
  console.error('Discord client error:', error);
});

client.on('shardError', (error) => {
  console.error('Discord shard error:', error);
  discordReady = false;
});

client.on('shardDisconnect', (event, shardId) => {
  console.warn(`Discord shard ${shardId} disconnected with code ${event?.code ?? 'unknown'}.`);
  discordReady = false;
});

client.on('shardReconnecting', (shardId) => {
  console.warn(`Discord shard ${shardId} reconnecting.`);
  discordReady = false;
  scheduleDiscordLoginWatchdog();
});

console.log('Starting Discord login...');
scheduleDiscordLoginWatchdog();
client.login(DISCORD_TOKEN)
  .then(() => console.log('Discord login promise resolved.'))
  .catch((error) => {
    clearTimeout(discordLoginWatchdog);
    console.error('Discord login failed:', error);
    process.exit(1);
  });
