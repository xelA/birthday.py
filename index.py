import discord
import sys
import random
import asyncio
import time
import datetime
import re

from discord.ext import commands
from discord.ext.commands import errors, Cog
from utils import create_tables, sqlite, default

# Test DB before launching
tables = create_tables.creation(debug=True)
if not tables:
    sys.exit(1)

# Local variables for index.py
config = default.get("config.json")
bot = commands.Bot(
    command_prefix=config.prefix, prefix=config.prefix, command_attrs=dict(hidden=True),
    intents=discord.Intents(guilds=True, messages=True)
)


class BirthdayBot(Cog):
    def __init__(self, bot):
        self.bot = bot
        self.re_timestamp = r"^(0[0-9]|1[0-9]|2[0-9]|3[0-1])\/(0[1-9]|1[0-2])\/([1-2]{1}[0-9]{3})"
        self.db = sqlite.Database()
        print("Logging in...")

    @commands.Cog.listener()
    async def on_command_error(self, ctx, err):
        if isinstance(err, errors.MissingRequiredArgument) or isinstance(err, errors.BadArgument):
            helper = str(ctx.invoked_subcommand) if ctx.invoked_subcommand else str(ctx.command)
            await ctx.send_help(helper)

        elif isinstance(err, errors.CommandInvokeError):
            error = default.traceback_maker(err.original)
            await ctx.send(f"There was an error processing the command ;-;\n{error}")

        elif isinstance(err, errors.MaxConcurrencyReached):
            await ctx.send("You've reached max capacity of command usage at once, please finish the previous one...")

        elif isinstance(err, errors.CheckFailure):
            pass

        elif isinstance(err, errors.CommandOnCooldown):
            await ctx.send(f"This command is on cooldown... try again in {err.retry_after:.2f} seconds.")

        elif isinstance(err, errors.CommandNotFound):
            pass

    @commands.Cog.listener()
    async def on_ready(self):
        print(f'Ready: {self.bot.user} | Servers: {len(self.bot.guilds)}')
        await self.bot.change_presence(
            activity=discord.Activity(type=3, name="when your birthday is due 🎉🎂 (Prefix: b!)"),
            status=discord.Status.idle
        )

        while True:
            await asyncio.sleep(10)
            guild = self.bot.get_guild(config.guild_id)
            birthday_role = discord.Object(id=config.birthday_role_id)

            # Check if someone has birthday today
            birthday_today = self.db.fetch(
                "SELECT * FROM birthdays WHERE has_role=0 AND strftime('%m-%d', birthday) = strftime('%m-%d', 'now')"
            )
            if birthday_today:
                for g in birthday_today:
                    self.db.execute("UPDATE birthdays SET has_role=1 WHERE user_id=?", (g["user_id"],))
                    try:
                        user = guild.get_member(g["user_id"])
                        await user.add_roles(birthday_role, reason=f"{user} has birthday! 🎂🎉")
                        await self.bot.get_channel(config.announce_channel_id).send(
                            f"Happy birthday {user.mention}, have a nice birthday and enjoy your role today 🎂🎉"
                        )
                        print(f"Gave role to {user.name}")
                    except Exception:
                        pass  # meh, just skip it...

            birthday_over = self.db.fetch(
                "SELECT * FROM birthdays WHERE has_role=1 AND strftime('%m-%d', birthday) != strftime('%m-%d', 'now')"
            )
            for g in birthday_over:
                self.db.execute("UPDATE birthdays SET has_role=0 WHERE user_id=?", (g["user_id"],))
                try:
                    user = guild.get_member(g["user_id"])
                    await user.remove_roles(birthday_role, reason=f"{user} does not have birthday now...")
                    print(f"Removed role from {user.name}")
                except Exception:
                    pass  # meh, just skip it...

    def check_birthday_noted(self, user_id):
        """ Convert timestamp string to datetime """
        data = self.db.fetchrow("SELECT * FROM birthdays WHERE user_id=?", (user_id,))
        if data:
            return data["birthday"]
        else:
            return None

    def calculate_age(self, born):
        """ Calculate age (datetime) """
        today = datetime.datetime.utcnow()
        age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        return age

    def ifelse(self, statement, if_statement: str, else_statement: str = None):
        """ Make it easier with if/else cases of what grammar to use
        - if_statement is returned when statement is True
        - else_statement is returned when statement is False/None
        """
        else_statement = else_statement if else_statement else ""
        return if_statement if statement else else_statement

    @commands.command()
    async def ping(self, ctx):
        """ Pong! """
        before = time.monotonic()
        before_ws = int(round(self.bot.latency * 1000, 1))
        message = await ctx.send("Loading...")
        ping = int((time.monotonic() - before) * 1000)
        await message.edit(content=f"🏓 WS: {before_ws}ms  |  REST: {ping}ms")

    @commands.command()
    async def time(self, ctx):
        """ Check what the time is for me (the bot) """
        time = datetime.datetime.utcnow().strftime("%d %B %Y, %H:%M")
        await ctx.send(f"Currently the time for me is **{time}**")

    @commands.command()
    async def source(self, ctx):
        """ Check out my source code <3 """
        # Do not remove this command, this has to stay due to the GitHub LICENSE.
        # TL:DR, you have to disclose source according to GNU GPL v3.
        # Reference: https://github.com/AlexFlipnote/birthday.py/blob/master/LICENSE
        await ctx.send(f"**{ctx.bot.user}** is powered by this source code:\nhttps://github.com/AlexFlipnote/birthday.py")

    @commands.command(aliases=['b', 'bd', 'birth', 'day'])
    async def birthday(self, ctx, *, user: discord.Member = None):
        """ Check your birthday or other people """
        user = user or ctx.author

        if user.id == self.bot.user.id:
            return await ctx.send("I have birthday **06 February**, thank you for asking ❤")

        has_birthday = self.check_birthday_noted(user.id)
        if not has_birthday:
            return await ctx.send(f"**{user.name}** has not saved his/her birthday :(")

        birthday = has_birthday.strftime('%d %B')
        age = self.calculate_age(has_birthday)

        is_author = user == ctx.author
        target = self.ifelse(is_author, "**You** have", f"**{user.name}** has")
        grammar = self.ifelse(is_author, "you're", "is")
        when = self.ifelse(is_author, " ", " on ")

        await ctx.send(
            f"{target} birthday{when}**{birthday}** and {grammar} currently **{age}** years old."
        )

    @commands.command()
    @commands.max_concurrency(1, per=commands.BucketType.user)
    async def set(self, ctx):
        """ Set your birthday :) """
        has_birthday = self.check_birthday_noted(ctx.author.id)
        if has_birthday:
            return await ctx.send(
                f"You've already set your birth date to **{has_birthday.strftime('%d %B %Y')}**\n"
                f"To change this, please contact the owner of the bot."
            )

        start_msg = await ctx.send(f"Hello there **{ctx.author.name}**, please enter when you were born. `[ DD/MM/YYYY ]`")
        confirmcode = random.randint(10000, 99999)

        def check_timestamp(m):
            if (m.author == ctx.author and m.channel == ctx.channel):
                if re.compile(self.re_timestamp).search(m.content):
                    return True
            return False

        def check_confirm(m):
            if (m.author == ctx.author and m.channel == ctx.channel):
                if (m.content.startswith(str(confirmcode))):
                    return True
            return False

        try:
            user = await self.bot.wait_for('message', timeout=30.0, check=check_timestamp)
        except asyncio.TimeoutError:
            return await start_msg.edit(
                content=f"~~{start_msg.clean_content}~~\n\nfine then, I won't save your birthday :("
            )

        timestamp = datetime.datetime.strptime(user.content.split(" ")[0], "%d/%m/%Y")
        timestamp_clean = timestamp.strftime("%d %B %Y")
        today = datetime.datetime.now()
        age = self.calculate_age(timestamp)

        if timestamp > today:
            return await ctx.send(f"Nope.. you can't exist in the future **{ctx.author.name}**")
        if age > 122:
            return await ctx.send(f"The world record for oldest human is **122**, doubtful you're that old **{ctx.author.name}**...")
        if age <= 12:
            return await ctx.send(f"You have to be **13** to use Discord **{ctx.author.name}**, are you saying you're underage? 🤔")

        confirm_msg = await ctx.send(
            f"Alright **{ctx.author.name}**, do you confirm that your birth date is **{timestamp_clean}** "
            f"and you're currently **{age}** years old?\nType `{confirmcode}` to confirm this choice\n"
            f"(NOTE: To change birthday later, you must send valid birthday to the owner)"
        )

        try:
            user = await self.bot.wait_for('message', timeout=30.0, check=check_confirm)
        except asyncio.TimeoutError:
            return await confirm_msg.edit(
                content=f"~~{confirm_msg.clean_content}~~\n\nStopped process..."
            )

        self.db.execute("INSERT INTO birthdays VALUES (?, ?, ?)", (ctx.author.id, timestamp, False))
        await ctx.send(f"Done, your birth date is now saved in my database at **{timestamp_clean}** 🎂")

    @commands.command()
    @commands.check(default.is_owner)
    async def forceset(self, ctx, user: discord.Member, *, time: str):
        timestamp = datetime.datetime.strptime(time, "%d/%m/%Y")
        data = self.db.execute("UPDATE birthdays SET birthday=? WHERE user_id=?", (timestamp, user.id))
        await ctx.send(data)

    @commands.command()
    @commands.check(default.is_owner)
    async def db(self, ctx, *, query: str):
        data = self.db.execute(query)
        await ctx.send(f"{data}")

    @commands.command()
    @commands.check(default.is_owner)
    async def dropall(self, ctx):
        data = self.db.execute("DELETE FROM birthdays")
        await ctx.send(f"DEBUG: {data}")

    @commands.command()
    @commands.check(default.is_owner)
    async def dropuser(self, ctx, *, user: discord.Member):
        data = self.db.execute("DELETE FROM birthdays WHERE user_id=?", (user.id,))
        await ctx.send(f"DEBUG: {data}")


bot.add_cog(BirthdayBot(bot))
bot.run(config.token)
